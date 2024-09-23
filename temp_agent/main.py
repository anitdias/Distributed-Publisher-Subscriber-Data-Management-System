import yaml
import psutil
import subprocess
import time
import docker
import uvicorn
from fastapi import FastAPI, Request
from kubernetes import client, config, utils
from kubernetes.client.rest import ApiException
import os

app = FastAPI()

# Initialize Kubernetes or regular config
if os.getenv("KUBERNETES_SERVICE_HOST"):
    config.load_incluster_config()
else:
    config.load_kube_config()


# Kubernetes-related functions
def create_publisher_deployment():
    apps_v1 = client.AppsV1Api()
    deployment_manifest = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "publisher-deployment"
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {
                    "app": "publisher"
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        "app": "publisher"
                    }
                },
                "spec": {
                    "containers": [
                        {
                            "name": "publisher",
                            "image": "publisher",
                            "imagePullPolicy": "Never",
                            "volumeMounts": [
                                {
                                    "name": "config-volume",
                                    "mountPath": "/publisher"
                                }
                            ]
                        }
                    ],
                    "volumes": [
                        {
                            "name": "config-volume",
                            "configMap": {
                                "name": "publisher-config"
                            }
                        }
                    ]
                }
            }
        }
    }

    try:
        apps_v1.create_namespaced_deployment(namespace="default", body=deployment_manifest)
        print("Publisher deployment created successfully.")
        return True  # Indicate that a new deployment was created
    except ApiException as e:
        print(f"Exception when creating deployment: {e}")
        return False  # Deployment not created due to an error


def check_and_create_publisher_deployment():
    apps_v1 = client.AppsV1Api()
    try:
        # Check if the deployment exists
        apps_v1.read_namespaced_deployment(name="publisher-deployment", namespace="default")
        print("Publisher deployment already exists.")
        return False  # Deployment already exists
    except ApiException as e:
        if e.status == 404:
            # If the deployment doesn't exist, create it
            print("Publisher deployment not found, creating it.")
            return create_publisher_deployment()  # Create deployment and return True if created
        else:
            print(f"Exception when checking deployment: {e}")
            return False  # Error occurred, deployment wasn't created


def update_k8s_config_map(config_map_name, new_topic, new_omit):
    v1 = client.CoreV1Api()
    config_map = v1.read_namespaced_config_map(config_map_name, 'default')

    current_config = yaml.safe_load(config_map.data['config.yml'])
    current_config['client']['topic'] = new_topic
    current_config['omit']['omit_time'] = new_omit
    config_map.data['config.yml'] = yaml.safe_dump(current_config)

    v1.patch_namespaced_config_map(config_map_name, 'default', config_map)
    print(f"ConfigMap {config_map_name} updated successfully.")


def restart_k8s_pod(deployment_name):
    apps_v1 = client.AppsV1Api()
    deployment = apps_v1.read_namespaced_deployment(deployment_name, 'default')
    deployment.spec.template.metadata.annotations = {
        'kubectl.kubernetes.io/restartedAt': time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    }
    apps_v1.patch_namespaced_deployment(deployment_name, 'default', deployment)
    print(f"Deployment {deployment_name} restarted successfully.")


# Docker and Linux-related functions
docker_client = docker.from_env()


def find_process_by_name(process_name):
    for proc in psutil.process_iter(['pid', 'name']):
        proc_info = proc.as_dict(attrs=['pid', 'name'])
        if proc_info['name'] == process_name:
            return proc_info['pid']
    return None


def is_process_running(process_name):
    return find_process_by_name(process_name) is not None


def start_service(service_name):
    try:
        subprocess.run(['systemctl', 'start', service_name], check=True, text=True, capture_output=True)
        print(f"Service {service_name} started successfully.")
    except Exception as e:
        print(f"Failed to start service: {e}")


def start_process_if_not_running(process_name, executable_path):
    if not is_process_running(process_name):
        start_service(process_name)
        time.sleep(2)


def stop_process(process_name):
    pid = find_process_by_name(process_name)
    if pid:
        try:
            proc = psutil.Process(pid)
            proc.terminate()
            proc.wait(timeout=10)
            print(f"Process {process_name} stopped successfully.")
        except psutil.NoSuchProcess:
            print(f"No such process: {process_name}")
        except psutil.AccessDenied:
            print(f"Access denied to terminate process: {process_name}")
        except psutil.TimeoutExpired:
            print(f"Timeout expired while stopping process: {process_name}")
    else:
        print(f"Process {process_name} not found.")


def create_and_start_container(container_name, image_name, volumes):
    try:
        container = docker_client.containers.run(
            image_name,
            name=container_name,
            volumes=volumes,
            detach=True
        )
        print(f"Container {container_name} created and started successfully.")
    except docker.errors.APIError as e:
        print(f"Error interacting with Docker API: {e}")


def start_container(container_name, image_name, volumes):
    try:
        container = docker_client.containers.get(container_name)
        if container.status != 'running':
            container.start()
            print(f"Container {container_name} started successfully.")
        else:
            print(f"Container {container_name} is already running.")
    except docker.errors.NotFound:
        create_and_start_container(container_name, image_name, volumes)
    except docker.errors.APIError as e:
        print(f"Error interacting with Docker API: {e}")


def stop_container(container_name):
    try:
        container = docker_client.containers.get(container_name)
        if container.status == 'running':
            container.stop()
            print(f"Container {container_name} stopped successfully.")
        else:
            print(f"Container {container_name} is not running.")
    except docker.errors.NotFound:
        print(f"Container {container_name} not found.")
    except docker.errors.APIError as e:
        print(f"Error interacting with Docker API: {e}")


def restart_container(container_name, image_name, volumes):
    stop_container(container_name)
    time.sleep(2)
    start_container(container_name, image_name, volumes)


def update_config(new_topic, new_omit, os_type):
    if os_type == 'kubernetes':
        update_k8s_config_map('publisher-config', new_topic, new_omit)
        deployment_created = check_and_create_publisher_deployment()
        if not deployment_created:
            restart_k8s_pod('publisher-deployment')
    else:
        config_path_publisher = '/publisher/config.yml'
        config_path_subscriber = '/subscriber/config.yml'
        with open(config_path_publisher, 'r+') as file:
            conf_data = yaml.safe_load(file)
            conf_data['omit']['omit_time'] = new_omit
            conf_data['client']['topic'] = new_topic
            file.seek(0)
            yaml.safe_dump(conf_data, file)
            file.truncate()

        with open(config_path_subscriber, 'r+') as file:
            conf_data = yaml.safe_load(file)
            conf_data['omit']['omit_time'] = new_omit
            conf_data['client']['topic'] = new_topic
            file.seek(0)
            yaml.safe_dump(conf_data, file)
            file.truncate()

        if os_type == 'linux':
            restart_process('subscriber', '/DATA/APS/subscriber/subscriber')
            restart_process('publisher', '/DATA/APS/publisher/publisher')
        elif os_type == 'docker':
            volumes_subscriber = {'subscriber_volume': {'bind': '/subscriber', 'mode': 'rw'}}
            volumes_publisher = {'publisher_volume': {'bind': '/publisher', 'mode': 'rw'}}
            restart_container('subscriber', 'subscriber', volumes_subscriber)
            restart_container('publisher', 'publisher', volumes_publisher)


@app.post('/agent')
async def start_agent(request: Request):
    config_data = await request.json()
    new_topic = config_data.get('topic')
    new_omit = config_data.get('omit_time')
    os_type = config_data.get('os')

    update_config(new_topic, new_omit, os_type)
    return {"status": "success"}


if __name__ == '__main__':
    uvicorn.run("main:app", host="0.0.0.0", port=8111)
