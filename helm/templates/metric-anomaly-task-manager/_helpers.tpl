{{- define "metricanomalytaskmanager.image" -}}
  {{- if and .Values.metricAnomalyTaskManagerConfig.image.tagOverride  -}}
    {{- printf "%s:%s" .Values.metricAnomalyTaskManagerConfig.image.repository .Values.metricAnomalyTaskManagerConfig.image.tagOverride }}
  {{- else -}}
    {{- printf "%s:%s" .Values.metricAnomalyTaskManagerConfig.image.repository .Chart.AppVersion }}
  {{- end -}}
{{- end -}}