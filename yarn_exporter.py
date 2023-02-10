import requests
from prometheus_client import start_http_server, Summary
from prometheus_client import Gauge
import time
import json

YARN_HOST = "http://mtg8-mt-01.tap.true.th:8088"

headers = {"Authorization": f"Bearer ", "Accept-Charst": "UTF-8"}

# Create a metric to track time spent and requests made.
REQUEST_TIME = Summary("request_processing_seconds", "Time spent processing request")

# Decorate function with metric.
@REQUEST_TIME.time()
def process_request(t):
    """A dummy function that takes some time."""
    time.sleep(t)

if __name__ == "__main__":
    # Start up the server to expose the metrics.
    start_http_server(8000)
    
    yarn_reason_cluster = Gauge(
        "YARN_Reason_Cluster",
        "reason_using",
        ["Reason_name","Sub_reason"],
    )
    yarn_reason = Gauge(
        "YARN_Reason",
        "reason_using",
        ["Reason_name","Sub_reason"],
    )

    yarn_sub_reason = Gauge(
        "YARN_Sub_Reason",
        "reason_using",
        ["Reason_name","Sub_reason"],
    )
    while True:
        yarn = requests.get(
                    f"{YARN_HOST}/ws/v1/cluster/scheduler", headers=headers
                )
        yarn_api = yarn.json()
        reason_all_using = yarn_api['scheduler']['schedulerInfo']['usedCapacity']
        print("Using all :",reason_all_using,"%")
        # Queue = yarn_api['scheduler']['schedulerInfo']['queues']
        # print(Queue)
        yarn_reason_cluster.labels(
            Reason_name= "Clusters",
            Sub_reason=""
            ).set(reason_all_using)
        
        for schedulerInfo in yarn_api['scheduler']['schedulerInfo']['queues']['queue']:
            Reason_name = schedulerInfo['queueName']
            Reason_using = schedulerInfo['usedCapacity']
            print(" Queue Name",schedulerInfo['queueName'])
            print(" Using",schedulerInfo['usedCapacity'],"%")
            print("")

            yarn_reason.labels(
                Reason_name=Reason_name, 
                Sub_reason=""
            ).set(Reason_using)
            
            # if schedulerInfo['queueName'] != "default" :
            if 'queues' in schedulerInfo:
                for subschedulerInfo in schedulerInfo['queues']['queue']:
                    sub_reason_name = subschedulerInfo['queueName']
                    sub_reason_using = subschedulerInfo['usedCapacity']
                    print("  Subqueue Name",subschedulerInfo['queueName'])
                    print("  Using",subschedulerInfo['usedCapacity'],"%")
                    print("")

                    yarn_sub_reason.labels(
                        Reason_name=Reason_name, Sub_reason=sub_reason_name
                        ).set(sub_reason_using)



            # print(YARN_HOST)

    # policies_cluste_lists = requests.get(
    #     f"{YARN_HOST}/api/2.0/policies/clusters/list", headers=headers
    # )
    # Generate some requests.
    # Clusters_state = Gauge(
    #     "Clusters_databricks_state",
    #     "Status",
    #     ["Cluster_name", "Workspace"]
    #     )

    # while True:
    #         Clusters_state.labels(
    #             Cluster_name=clusters_name, Workspace=WORKSPACE_NAME
    #         ).set(status_cluster)
        process_request(5)
        print("--------------------------")