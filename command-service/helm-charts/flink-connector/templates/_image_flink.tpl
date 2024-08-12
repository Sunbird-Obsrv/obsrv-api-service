{{/* {{- include "base.image.flink" dict ("context" $ "scope" $jobData) }} */}}
{{- define "base.image.flink" }}
{{- $context := .context }}
{{- $scope := .scope }}
{{- with $scope }}
{{- $registry := default $context.Values.registry .registry }}
{{- $repository := default $context.Values.repository .repository }}
{{- $image := printf "%s/%s" $registry $repository}}
{{- if .digest }}
{{- printf "%s@%s" $image .digest }}
{{- else }}
{{- $tag := default $context.Values.tag .tag }}
{{- printf "%s:%s" $image $tag }}
{{- end }}
{{- end }}
{{- end }}
