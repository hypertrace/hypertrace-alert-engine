{{- define "service.image" -}}
  {{- if and .name.image.tagOverride  -}}
    {{- printf "%s:%s" .name.image.repository .name.image.tagOverride }}
  {{- else -}}
    {{- printf "%s:%s" .name.image.repository .context.Chart.AppVersion }}
  {{- end -}}
{{- end -}}