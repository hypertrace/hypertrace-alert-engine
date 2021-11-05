{{- define "hypertracealertengine.image" -}}
  {{- if and .Values.hypertraceAlertEngineConfig.image.tagOverride  -}}
    {{- printf "%s:%s" .Values.hypertraceAlertEngineConfig.image.repository .Values.hypertraceAlertEngineConfig.image.tagOverride }}
  {{- else -}}
    {{- printf "%s:%s" .Values.hypertraceAlertEngineConfig.image.repository .Chart.AppVersion }}
  {{- end -}}
{{- end -}}