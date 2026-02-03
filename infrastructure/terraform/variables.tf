variable "credentials" {
  description = "My Credentials"
  default     = "/Users/biancaniemann/.keys/secret_key.json"
}

variable "project" {
  description = "Project"
  default     = "marketplace-analytics-485915"
}

variable "location" {
  description = "My GCP Location"
  default     = "EU"
}

variable "raw_dataset_name" { 
    description = "Raw dataset name" 
    default = "raw" 
} 

variable "analytics_dataset_name" { 
    description = "Analytics dataset name" 
    default = "analytics" 
}

variable "gsc_storage_class" {
  description = "My GSC Storage Class"
  default     = "STANDARD"
}

#variable "gsc_bucket_name" {
#  description = "My GSC Bucket Name"
#  default     = "terraform-demo-bucket-484515"
#}

variable "region" {
  description = "My GCP Region"
  default     = "europe-west3"
}
