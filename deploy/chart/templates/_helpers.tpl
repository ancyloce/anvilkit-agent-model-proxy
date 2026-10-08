{{/* The stable service identity (architecture.md naming): Deployment, Service,
ServiceAccount and Helm release share it. */}}
{{- define "anvilkit-agent-model-proxy.name" -}}
anvilkit-agent-model-proxy
{{- end -}}

{{- define "anvilkit-agent-model-proxy.fullname" -}}
{{- if eq .Release.Name (include "anvilkit-agent-model-proxy.name" .) -}}
{{- .Release.Name -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "anvilkit-agent-model-proxy.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-model-proxy.labels" -}}
app.kubernetes.io/name: {{ include "anvilkit-agent-model-proxy.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/component: model-proxy
app.kubernetes.io/part-of: anvilkit
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{- define "anvilkit-agent-model-proxy.selectorLabels" -}}
app.kubernetes.io/name: {{ include "anvilkit-agent-model-proxy.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "anvilkit-agent-model-proxy.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "anvilkit-agent-model-proxy.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* The image reference: a digest pins the exact image, otherwise the tag
(defaulting to the chart's appVersion). */}}
{{- define "anvilkit-agent-model-proxy.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{/* The rendered configuration: the reviewed sections with the identity
modes of the environment, the s3 store backend and the mounted file paths. */}}
{{- define "anvilkit-agent-model-proxy.config" -}}
{{- $cfg := deepCopy .Values.config -}}
{{- $_ := set $cfg.store "backend" "s3" -}}
{{- $_ = set $cfg "development" (dict "enabled" .Values.development.enabled) -}}
{{- $identity := dict "mode" .Values.identity.mode "owner" $cfg.identity.owner -}}
{{- $mtls := dict "principals" $cfg.identity.mtls.principals -}}
{{- if eq .Values.identity.mode "mtls" -}}
{{- $_ = set $mtls "cert_file" "/etc/anvilkit/model-proxy-mtls/tls.crt" -}}
{{- $_ = set $mtls "key_file" "/etc/anvilkit/model-proxy-mtls/tls.key" -}}
{{- $_ = set $mtls "ca_file" "/etc/anvilkit/model-proxy-mtls/ca.crt" -}}
{{- end -}}
{{- $_ = set $identity "mtls" $mtls -}}
{{- $_ = set $cfg "identity" $identity -}}
{{- $controlIdentity := dict "mode" .Values.control.identity.mode -}}
{{- if eq .Values.control.identity.mode "mtls" -}}
{{- $_ = set $controlIdentity "mtls" (dict "cert_file" "/etc/anvilkit/control-mtls/tls.crt" "key_file" "/etc/anvilkit/control-mtls/tls.key" "ca_file" "/etc/anvilkit/control-mtls/ca.crt" "server_name" .Values.control.identity.mtlsSecret.serverName) -}}
{{- end -}}
{{- $_ = set $cfg.control "identity" $controlIdentity -}}
{{- toYaml $cfg -}}
{{- end -}}

{{/* Required environment values, checked once for every template. */}}
{{- define "anvilkit-agent-model-proxy.require" -}}
{{- if not .Values.control.address }}
{{- fail "control.address is required: Control's DispatchService (ANVILKIT_MODEL_PROXY_CONTROL_ADDRESS)" }}
{{- end }}
{{- if or (not .Values.store.s3.endpoint) (not .Values.store.s3.bucket) (not .Values.store.s3.secret.name) }}
{{- fail "store.s3.endpoint, store.s3.bucket and store.s3.secret.name are required: the shared record and evidence store every replica uses (ANVILKIT_MODEL_PROXY_STORE_S3_*)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "development") (not .Values.identity.principalsSecret.name) }}
{{- fail "identity.principalsSecret.name is required under identity.mode development: an existing Secret holding the DEVELOPMENT_ONLY bearer principals file" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") (not .Values.identity.certificate.create) (not .Values.identity.mtlsSecret.name) }}
{{- fail "identity.mtlsSecret.name is required under identity.mode mtls while identity.certificate.create is false: an existing Secret holding tls.crt, tls.key and ca.crt" }}
{{- end }}
{{- if and (eq .Values.control.identity.mode "mtls") (not .Values.identity.certificate.create) (not .Values.control.identity.mtlsSecret.name) }}
{{- fail "control.identity.mtlsSecret.name is required under control.identity.mode mtls while identity.certificate.create is false" }}
{{- end }}
{{- if not (has .Values.identity.mode (list "development" "mtls")) }}
{{- fail "identity.mode must be development or mtls" }}
{{- end }}
{{- if not (has .Values.control.identity.mode (list "development" "mtls")) }}
{{- fail "control.identity.mode must be development or mtls" }}
{{- end }}
{{- if and (eq .Values.identity.mode "development") (not .Values.development.enabled) }}
{{- fail "identity.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true (bearer principals authenticate no workload)" }}
{{- end }}
{{- if and (eq .Values.control.identity.mode "development") (not .Values.development.enabled) }}
{{- fail "control.identity.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true (a plaintext Control transport)" }}
{{- end }}
{{- if and .Values.identity.certificate.create (or (eq .Values.identity.mode "mtls") (eq .Values.control.identity.mode "mtls")) (not .Values.identity.certificate.issuerRef.name) }}
{{- fail "identity.certificate.issuerRef.name is required: the cert-manager issuer of the workload certificate (or set identity.certificate.create false and the mtlsSecret names)" }}
{{- end }}
{{- if and (or (eq .Values.identity.mode "mtls") (eq .Values.control.identity.mode "mtls")) (not .Values.identity.trustDomain) (not .Values.development.enabled) }}
{{- fail "identity.trustDomain is required outside development (the development default anvilkit.local applies only with development.enabled: true)" }}
{{- end }}
{{- if and (eq .Values.control.identity.mode "mtls") (not .Values.control.identity.mtlsSecret.serverName) }}
{{- fail "control.identity.mtlsSecret.serverName is required under control.identity.mode mtls" }}
{{- end }}
{{- range .Values.routeCredentials }}
{{- if or (not .env) (not .secret.name) (not .secret.key) }}
{{- fail "every routeCredentials entry needs env, secret.name and secret.key" }}
{{- end }}
{{- end }}
{{- end -}}


{{/* The identity Secrets: the rendered Certificate's or the environment's. */}}
{{- define "anvilkit-agent-model-proxy.identitySecret" -}}
{{- if .Values.identity.certificate.create -}}
{{- printf "%s-identity" (include "anvilkit-agent-model-proxy.fullname" .) -}}
{{- else -}}
{{- .Values.identity.mtlsSecret.name -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-model-proxy.controlIdentitySecret" -}}
{{- if .Values.identity.certificate.create -}}
{{- printf "%s-identity" (include "anvilkit-agent-model-proxy.fullname" .) -}}
{{- else -}}
{{- .Values.control.identity.mtlsSecret.name -}}
{{- end -}}
{{- end -}}
