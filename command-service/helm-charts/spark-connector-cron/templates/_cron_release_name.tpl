{{- define "base.cronReleaseName" -}}
  {{- $name := printf "%s--%s" .Chart.Name .Values.instance_id }}
  {{- default .Values.instance_id $name }}
{{- end }}