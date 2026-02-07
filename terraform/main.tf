terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.26.0"
    }
  }
}

provider "databricks" {
  host  = "https://dbc-5a1f80d6-ed96.cloud.databricks.com"
  token = var.databricks_token
}

data "databricks_node_type" "smallest" {
  local_disk = true
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

# Single node cluster with Unity Catalog compatibility
resource "databricks_cluster" "single_node" {
  cluster_name            = "Single Node"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = data.databricks_node_type.smallest.id
  autotermination_minutes = 20
  num_workers             = 0
  
  # Add data security mode for Unity Catalog workspaces
  data_security_mode = "USER_ISOLATION"
  
  # Runtime engine for better performance
  runtime_engine = "STANDARD"
  
  aws_attributes {
    availability           = "SPOT"
    zone_id               = "auto"
    first_on_demand       = 1
    spot_bid_price_percent = 100
    
    ebs_volume_type  = "GENERAL_PURPOSE_SSD"
    ebs_volume_count = 1
    ebs_volume_size  = 32
  }
  
  spark_conf = {
    "spark.master" = "local[*]"
    "spark.databricks.cluster.profile" = "singleNode"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}