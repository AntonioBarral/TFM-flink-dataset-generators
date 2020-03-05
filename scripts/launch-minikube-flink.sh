#!/bin/bash
kubectl create -f ../minikube-flink-config-files/flink-configuration-configmap.yaml
kubectl create -f ../minikube-flink-config-files/jobmanager-service.yaml
kubectl create -f ../minikube-flink-config-files/jobmanager-deployment.yaml
kubectl create -f ../minikube-flink-config-files/taskmanager-deployment.yaml
