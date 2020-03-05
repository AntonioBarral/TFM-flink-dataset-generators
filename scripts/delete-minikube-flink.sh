#!/bin/bash
kubectl delete -f ../minikube-flink-config-files/flink-configuration-configmap.yaml
kubectl delete -f ../minikube-flink-config-files/jobmanager-service.yaml
kubectl delete -f ../minikube-flink-config-files/jobmanager-deployment.yaml
kubectl delete -f ../minikube-flink-config-files/taskmanager-deployment.yaml
