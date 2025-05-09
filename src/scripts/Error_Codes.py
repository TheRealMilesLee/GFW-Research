from ..Database.DBOperations import MongoDBHandler, ADC_db
import os
import re

ERROR_CODES = MongoDBHandler(ADC_db["ERROR_CODES"])


def parse_txt():
  """
    1. 读取/home/silverhand/Developer/SourceRepo/GFW-Research/Lib/AfterDomainChange/China-Mobile/Error文件夹下的txt文件
    2. 解析文件内容，文件格式如下：
      Error querying awsdns-09.com with 211.136.17.107: The resolution lifetime expired after 20.202 seconds: Server Do53:1.1.1.1@53 answered The DNS operation timed out after 10.000 seconds; Server Do53:1.1.1.1@53 answered The DNS operation timed out after 9.898 seconds
      对于上面的例子，解析后的结果为：
      domain: awsdns-09.com
      dns_server:211.136.17.107
      error_code: Timed out
      error_reason: The DNS operation timed out

      或者有可能是以下格式：
      Error querying investidor10.com.br with 180.76.76.76: All nameservers failed to answer the query patria.org.ve. IN A: Server Do53:211.136.17.107@53 answered SERVFAIL
      对于上面的例子，解析后的结果为：
      domain: investidor10.com.br
      dns_server: 180.76.76.76
      error_code: SERVFAIL
      error_reason: All nameservers failed to answer the query

    3. 将解析后的结果存入ERROR_CODES数据库
  """
  ERROR_CODES.delete_many({})
  # 读取文件夹下的所有txt文件
  path = "/home/lhengyi/Developer/GFW-Research/Lib/AfterDomainChange/China-Mobile/Error"
  files = os.listdir(path)
  for file in files:
    file_path = os.path.join(path, file)
    with open(file_path, "r") as file:
      lines = file.readlines()
      for line in lines:
        if "Error querying" in line:
          domain = line.split("Error querying ")[1].split(" with ")[0]
          # 使用更严格的分割方式获取dns_server
          server_part = line.split(" with ")[1].split(": ")[0]
          dns_server = server_part.split(":")[0]

          # 判断record_type
          if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", dns_server):
            record_type = "A"
          elif re.match(r"^[0-9a-fA-F:]+$", dns_server):
            record_type = "AAAA"
          else:
            record_type = "Unknown"

          # 新增对更多错误类型的判断
          if "NXDOMAIN" in line or "The DNS query name does not exist" in line:
            error_code = "NXDOMAIN"
            error_reason = "Domain does not exist"
          elif "REFUSED" in line:
            error_code = "REFUSED"
            error_reason = "Query refused by server"
          elif "FORMERR" in line:
            error_code = "FORMERR"
            error_reason = "Format error"
          elif "SERVFAIL" in line:
            error_code = "SERVFAIL"
            error_reason = "Server failed to respond"
          elif "timed out" in line or "The resolution lifetime expired" in line:
            error_code = "Timed out"
            error_reason = "The DNS operation timed out"
          elif "YXDOMAIN" in line:
            error_code = "YXDOMAIN"
            error_reason = "Domain exists but should not"
          elif "YXRRSET" in line:
            error_code = "YXRRSET"
            error_reason = "Resource record set exists but should not"
          elif "NOTIMP" in line:
            error_code = "NOTIMP"
            error_reason = "Query not implemented by server"
          elif "NOTAUTH" in line:
            error_code = "NOTAUTH"
            error_reason = "Server not authoritative for zone"

          record = {
              "_id":
              f"{domain}-{dns_server}-{error_code}-{error_reason}-{record_type}",
              "domain": domain,
              "dns_server": dns_server,
              "error_code": error_code,
              "error_reason": error_reason,
              "record_type": record_type
          }
          exists = ERROR_CODES.find_one({
              "_id":
              f"{domain}-{dns_server}-{error_code}-{error_reason}-{record_type}",
              "domain": domain,
              "dns_server": dns_server,
              "error_code": error_code,
              "error_reason": error_reason,
              "record_type": record_type
          })
          if exists:
            ERROR_CODES.update_one({"_id": exists["_id"]}, {"$set": record},
                                   upsert=True)
          else:
            ERROR_CODES.insert_one(record)
    print(f"Finished processing {file_path}")
    print(f"Total records: {ERROR_CODES.count_documents({})}")


if __name__ == "__main__":
  parse_txt()
