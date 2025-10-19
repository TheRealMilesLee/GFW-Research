// 连接到 admin 数据库
db = db.getSiblingDB("admin");

// 创建 admin 用户
db.createUser({
  user: "admin",
  pwd: "TheMilesLee710", // 请替换为实际密码
  roles: [{ role: "userAdminAnyDatabase", db: "admin" }]
});

// 使用 admin 用户身份进行认证
db.auth("admin", "TheMilesLee710");

// =============== BeforeDomainChange 数据库 ===============
db = db.getSiblingDB("BeforeDomainChange");

db.createCollection("China-Mobile-DNSPoisoning");
db.createCollection("China-Mobile-GFWLocation");
db.createCollection("China-Mobile-IPBlocking");

db.createCollection("China-Telecom-IPBlocking");

db.createCollection("UCDavis-CompareGroup-DNSPoisoning");
db.createCollection("UCDavis-CompareGroup-GFWLocation");
db.createCollection("UCDavis-CompareGroup-IPBlocking");

// =============== AfterDomainChange 数据库 ===============
db = db.getSiblingDB("AfterDomainChange");

db.createCollection("China-Mobile-DNSPoisoning");
db.createCollection("China-Mobile-GFWLocation");

db.createCollection("China-Telecom-DNSPoisoning");
db.createCollection("China-Telecom-GFWLocation");
db.createCollection("China-Telecom-IPBlocking");

db.createCollection("UCDavis-Server-GFWLocation");
db.createCollection("UCDavis-Server-IPBlocking");
db.createCollection("UCDavis-Server-DNSPoisoning");

// 打印所有数据库，确认创建完成
print("已创建的数据库列表：");
printjson(db.getMongo().getDBs());

