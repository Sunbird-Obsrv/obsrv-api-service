{{/*
Expand the name of the chart.
*/}}
{{- define "connector-cron-jobs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "connector-cron-jobs.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "connector-cron-jobs.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "connector-cron-jobs.labels" -}}
helm.sh/chart: {{ include "connector-cron-jobs.chart" . }}
{{ include "connector-cron-jobs.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "connector-cron-jobs.selectorLabels" -}}
app.kubernetes.io/name: {{ include "connector-cron-jobs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

# .Values.namespace will get overridden by .Values.global.namespace.chart-name
{{- define "base.namespace" -}}
  {{- $chartName := .Chart.Name }}
  {{- $namespace := default .Release.Namespace .Values.namespace }}
  {{- if .Values.global }}
  {{- with .Values.global.namespace }}
    {{- if hasKey . $chartName }}
      {{- $namespace = index . $chartName }}
    {{- end }}
  {{- end }}
  {{- end }}
  {{- $namespace | trunc 63 | trimSuffix "-" }}
{{- end }}


{{/*
Create the name of the service account to use
*/}}
{{- define "base.serviceaccountname" -}}
  {{- $name := printf "%s-%s" .Chart.Name "sa" }}
  {{- default $name .Values.serviceAccount.name }}
{{- end }}
