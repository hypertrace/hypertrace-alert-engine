{{- define "notificationservice.image" -}}
  {{- if and .Values.notificationServiceConfig.image.tagOverride  -}}
    {{- printf "%s:%s" .Values.notificationServiceConfig.image.repository .Values.notificationServiceConfig.image.tagOverride }}
  {{- else -}}
    {{- printf "%s:%s" .Values.notificationServiceConfig.image.repository .Chart.AppVersion }}
  {{- end -}}
{{- end -}}