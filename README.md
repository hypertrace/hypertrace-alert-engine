# Alerting

## Introduction
With Hypertrace we are moving in a direction where we promote it more like an Observability platform than just a distributed tracing tool similar to Jaeger or Zipkin. So when we talk about observability, alerting is one of the main components of it. As of now we provide support for the following 
tasks. More features would be added as we progress along in time.

- Get an alert whenever there is a sudden spike/ sharp increase in latency of any operation/ API calls from or to my service.
- Get an alert whenever there is a sudden spike in traffic/ increase in call rate to my service/ API. 
- Get an alert whenever there is a sudden spike in errors/ increase in error rate for my service. 

## Design
Currently, we support setting up the rules via a config or via deployment. However, in the UI, we will have a way to list the alert rules and notification configuration. If we closely look at the alerting service, we see that it is simply an anomaly condition from a baseline for a particular metric. So at a high level we need a **metric anomaly definition** (alert rule) which consists of metric selection and baseline calculation. Once we have designed our alert rule, we would need a **anomaly evaluation engine** over a metric (alert rule evaluation engine) and an **alert notification engine** (notification sender) to send the notification.

### Alert rule description
Below is a sample alert rule.
```
[
  {
    "id": "notification_rule_1",
    "ruleName": "high_avg_latency",
    "description": "Alert for high avg latency of payment service",
    "eventConditionId": "event_condition_1",
    "eventConditionType": "MetricAnomalyEventCondition",
    "channelId": "channel-id-1",
    "eventCondition" :  {
      "metricSelection": {
        "metricAttribute": {
          "key": "duration",
          "scope": "SERVICE"
        },
        "metricAggregationFunction": "METRIC_AGGREGATION_FUNCTION_TYPE_AVG",
        "metricAggregationInterval": "PT15S",
        "filter": {
          "leafFilter": {
            "lhsExpression": {
              "attribute": {
                "key": "name",
                "scope": "SERVICE"
              }
            },
            "valueOperator": "VALUE_OPERATOR_EQ",
            "rhsExpression": {
              "stringValue": "customer"
            }
          }
        }
      },
      "violationCondition": [{
        "staticThresholdCondition": {
          "operator": "STATIC_THRESHOLD_OPERATOR_GT",
          "value": 5.0,
          "minimumViolationDuration": "PT5S",
          "severity": "SEVERITY_CRITICAL"
        }
      }]
    }
  }
]
```
Various fields of the rule are:
- id : It specifies the rule id
- rulename: It specifies the rule name
- description: It specifies the description of the rule
- eventConditionId: It refers to id of the event condition for which we want the alert
- eventConditionType: It refers to the type of event condition for which we want the alert
- channelId: It refers to the channel over which notifications would be sent (should have a corressponding notification config)
- eventCondition: It specifies the exact event based on which alerts would be rolled out. It has two main parts:

  1. metricSelection

  - First we specify the metric attribute and its scope using *metricAttribute* field. Currently we support alerts on metrics like numCall / duration / error count where the scope could be at API or SERVICE level.
    
  - Then we need to specify the metric aggretgation function which would be used to aggregate the data points over the specified time interval (granularity). This can be configured using *metricAggregationFunction* and *metricAggregationInterval* fields respectively.

  - Next we need to select a filter to be applied on the respective SERVICE/API as there can be multiple services/apis. As an example the above rule uses the filter : "Select the service where service name is customer".

  2. violationCondition/baseline

      Baseline can be fixed or dynamic (based on past data points). Currently we support fixed baseline based on which anomaly would be decided. As an example the above rule uses the baseline : "Raise an anomaly when the selected metric value is greater than 5 for all the points in the 15s duration"

### Metric Anomaly Evaluation Engine
At a time there can be multiple rules configured. So we need a mechanism to evaluate all the rules and at all the times for alert to be useful. We have an evaluation engine which in background, keeps on evaluating all the rules in a periodic fashion at every 1 minute interval. At a high level, for a given rule the evaluation will look like as:

1. Fetch all the metric points of metric as per selection definition for current 1 min duration by firing query to the data store (pinot in our case). So, if granularity is 15s, there will be a maximum of 4 points will be received, and if it's 1 min, there will be 1 data point.

2. Similarly, fetch all the metrics points for the required baseline.

3. Evaluate all the current metric data points against the violation condition. If all points violated, raises an alert.


### Alert Notification Engine
Currently we support sending alert notification via slack.


## How to build
