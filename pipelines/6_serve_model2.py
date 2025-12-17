import yaml
from kubernetes import client, config

raw_isvc = """
apiVersion: "serving.kserve.io/v1beta1"
kind: "InferenceService"
metadata:
  name: "lgbm-sales-pred"
  namespace: kserve
  annotations:
    serving.kserve.io/deploymentMode: "RawDeployment"
spec:
  predictor:
    initContainers:
      - image: rclone/rclone:master
        name: fetch-model
        args:
          - "copy"
          - "s3:datalake/models/lightgbm.pkl"
          - "/mnt/models"
          - "--update"
        envFrom:
          - secretRef:
              name: s3-secret
        volumeMounts:
          - name: rclone-conf
            mountPath: /config/rclone/
            readOnly: True
          - name: model-pvc
            mountPath: /mnt/models
            readOnly: False
    containers:
      - image: docker.io/maulanaysfi/model-runtime:0.2.2
        name: model-runtime
        resources:
          requests:
            cpu: 50m
            memory: 256Mi
          limits:
            cpu: 200m
            memory: 1Gi
        env:
          - name: MODEL_PATH
            valueFrom:
              configMapKeyRef:
                name: lgbm-sales-pred-config
                key: model-path
        ports:
          - name: http
            containerPort: 5001
            protocol: TCP
        readinessProbe:
          httpGet:
            path: /
            port: 5001
          initialDelaySeconds: 5
          periodSeconds: 10
        volumeMounts:
          - name: model-pvc
            mountPath: /mnt/models
            readOnly: False
    volumes:
      - name: rclone-conf
        configMap:
          name: lgbm-sales-pred-config
          items:
            - key: rclone.conf
              path: rclone.conf
      - name: model-pvc
        persistentVolumeClaim:
          claimName: lgbm-sales-pred-model
    minReplicas: 0
    maxReplicas: 5
    scaleTarget: 150
    scaleMetric: cpu
"""

# config.load_incluster_config()
config.load_kube_config("/home/ibrahim/.kube/config")
api = client.CustomObjectsApi()

GROUP = "serving.kserve.io"
VERSION = "v1beta1"
PLURAL = "inferenceservices"
NAMESPACE = "kserve"

isvc = yaml.safe_load(raw_isvc)
name = isvc["metadata"]["name"]

try:
    api.get_namespaced_custom_object(
        group=GROUP,
        version=VERSION,
        namespace=NAMESPACE,
        plural=PLURAL,
        name=name,
    )

    api.patch_namespaced_custom_object(
        group=GROUP,
        version=VERSION,
        namespace=NAMESPACE,
        plural=PLURAL,
        name=name,
        body=isvc,
    )
    print(f"InferenceService '{name}' patched.")

except client.exceptions.ApiException as e:
    if e.status == 404:
        api.create_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=NAMESPACE,
            plural=PLURAL,
            body=isvc,
        )
        print(f"InferenceService '{name}' created.")
    else:
        raise
