kubectl annotate nodes --overwrite master topology-aware-scheduling.cs.phd.uqtr/available_disk=10240 
kubectl annotate nodes --overwrite edge1 topology-aware-scheduling.cs.phd.uqtr/available_disk=10240
kubectl annotate nodes --overwrite edge2 topology-aware-scheduling.cs.phd.uqtr/available_disk=10240
kubectl annotate nodes --overwrite edge3 topology-aware-scheduling.cs.phd.uqtr/available_disk=10240
kubectl annotate nodes --overwrite edge4 topology-aware-scheduling.cs.phd.uqtr/available_disk=10240
