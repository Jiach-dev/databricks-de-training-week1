# Week 1: Databricks Environment Setup Notes

## 🔹 Workspace Details
- **Workspace Name:** Bricks_space  
- **Region:** Eastus  
- **Deployment Platform:** Azure Databricks

## 🔹 Cluster Configuration
- **Cluster Name:** Jean-Hénock VIAYINON's Cluster  
- **Runtime Version:** Databricks Runtime 15.4 (Scala 2.12)  
- **Engine:** Photon  
- **Worker Node Type:** Standard_DS3_v2  
- **Autoscaling:** Enabled  
  - Minimum Workers: 1  
  - Maximum Workers: 2  
- **Autotermination:** 120 minutes  
- **Spark Environment Variable:**  
  - `PYSPARK_PYTHON`: `/databricks/python3/bin/python3`  
- **Security Mode:** Single-user  
- **Availability Mode:** Spot with On-Demand Fallback  
- **Spot Bid Max Price:** Default (-1)  
- **User:** henocksjean@gmail.com

## 🔹 Configuration Summary (JSON Extract)
```json
{
  "cluster_name": "Jean-Hénock VIAYINON's Cluster",
  "spark_version": "15.4.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "autoscale": {
    "min_workers": 1,
    "max_workers": 2
  },
  "autotermination_minutes": 120,
  "runtime_engine": "PHOTON",
  "spark_env_vars": {
    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
  },
  "data_security_mode": "SINGLE_USER"
}
