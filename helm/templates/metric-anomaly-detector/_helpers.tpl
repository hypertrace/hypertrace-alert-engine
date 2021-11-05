{{- define "metricanomalydetector.image" -}}
  {{- if and .Values.metricAnomalyDetectorConfig.image.tagOverride  -}}
    {{- printf "%s:%s" .Values.metricAnomalyDetectorConfig.image.repository .Values.metricAnomalyDetectorConfig.image.tagOverride }}
  {{- else -}}
    {{- printf "%s:%s" .Values.metricAnomalyDetectorConfig.image.repository .Chart.AppVersion }}
  {{- end -}}
{{- end -}}