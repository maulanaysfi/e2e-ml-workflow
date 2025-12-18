from datetime import datetime, timezone

import yaml
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

# ==============================================
# ===== inferenceService raw YAML manifest =====
# ==============================================
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

# ===========================
# ===== load kubeconfig =====
# ===========================
# config.load_incluster_config()
config.load_kube_config("/home/ibrahim/.kube/config")
api = client.CustomObjectsApi()

# ===========================
# ===== manifest config =====
# ===========================
isvc = yaml.safe_load(raw_isvc)
NAME = isvc["metadata"]["name"]
GROUP = "serving.kserve.io"
VERSION = "v1beta1"
PLURAL = "inferenceservices"
NAMESPACE = "kserve"

isvc["metadata"].setdefault("annotations", {})
isvc["metadata"]["annotations"]["rollout.kserve.io/restartedAt"] = datetime.now(
    timezone.utc
).isoformat()

# =============================
# ===== applying manifest =====
# =============================
try:
    api.patch_namespaced_custom_object(
        group=GROUP,
        version=VERSION,
        namespace=NAMESPACE,
        plural=PLURAL,
        name=NAME,
        body=isvc,
    )
    print(f"InferenceService '{NAME}' patched. (pod(s) should be rolled)")

except ApiException as e:
    if e.status == 404:
        api.create_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=NAMESPACE,
            plural=PLURAL,
            body=isvc,
        )
        print(f"InferenceService '{NAME}' created.")
    else:
        raise
