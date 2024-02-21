#!/bin/bash

# Output file
output_file="traceroute_log.txt"

# List of domains to trace
domains=("google.com" "truthsocial.com" "youtube.com" "facebook.com" "twitter.com" "ucdavis.edu" "nob.cs.ucdavis.edu")

# Function to perform traceroute and log the results
perform_traceroute() {
  domain=$1
  echo "Traceroute for $domain:" >> "$output_file"
  tracert "$domain" >> "$output_file"
  echo -e "\n--------------------------------------------\n" >> "$output_file"
}

# Perform traceroute for each domain
for domain in "${domains[@]}"; do
  perform_traceroute "$domain"
done

echo "Traceroute logs have been saved to $output_file"

