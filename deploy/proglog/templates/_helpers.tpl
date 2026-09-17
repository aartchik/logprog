{{/*
Имя приложения.
*/}}
{{- define "proglog.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Полное имя Kubernetes-ресурсов.
*/}}
{{- define "proglog.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Общие labels.
*/}}
{{- define "proglog.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
{{ include "proglog.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Labels, по которым StatefulSet/Service находят Pod'ы.
*/}}
{{- define "proglog.selectorLabels" -}}
app.kubernetes.io/name: {{ include "proglog.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
