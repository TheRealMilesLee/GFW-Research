// 连接 MongoDB 实例
use admin;

// 创建 admin 用户
db.createUser({
  user: "admin",
  pwd: "TheMilesLee710", // 请替换为实际密码
  roles: [{ role: "userAdminAnyDatabase", db: "admin" }]
});

// 以 admin 用户身份重新登录
db.auth("admin", "TheMilesLee710");

// 创建 BeforeDomainChange 数据库并创建集合
use BeforeDomainChange;

db.createCollection("China-Mobile-DNSPoisoning");
db.createCollection("China-Mobile-GFWLocation");
db.createCollection("China-Mobile-IPBlocking");

db.createCollection("China-Telecom-IPBlocking");

db.createCollection("UCDavis-CompareGroup-DNSPoisoning");
db.createCollection("UCDavis-CompareGroup-GFWLocation");
db.createCollection("UCDavis-CompareGroup-IPBlocking");

// 创建 AfterDomainChange 数据库并创建集合
use AfterDomainChange;

db.createCollection("China-Mobile-DNSPoisoning");
db.createCollection("China-Mobile-GFWLocation");

db.createCollection("China-Telecom-DNSPoisoning");
db.createCollection("China-Telecom-GFWLocation");
db.createCollection("China-Telecom-IPBlocking");

db.createCollection("UCDavis-Server-GFWLocation");
db.createCollection("UCDavis-Server-IPBlocking");
db.createCollection("UCDavis-Server-DNSPoisoning");

// 确保所有内容已创建
show dbs;
