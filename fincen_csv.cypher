create constraint on (c:Country) assert c.code is unique;
create constraint on (e:Entity) assert e.id is unique;
create constraint on (f:Filing) assert f.id is unique;
create index on :Filing(icij_sar_id);
create index on :Entity(name);
create index on :Filing(begin);
create index on :Filing(end);
create index on :Filing(amount);
create index on :Country(name);

load csv with headers from "https://raw.githubusercontent.com/jexp/fincen/main/download_transactions_map.csv" as value
merge (s:Filing {id:value.id}) set s += value;

load csv with headers from "https://raw.githubusercontent.com/jexp/fincen/main/download_bank_connections.csv" as value
match (f:Filing {icij_sar_id:value.icij_sar_id})
merge (filer:Entity {id:value.filer_org_name_id}) on create set filer.name = value.filer_org_name, 
filer.location = point({latitude:toFloat(value.filer_org_lat),longitude:toFloat(value.filer_org_lng)})
merge (other:Entity {id:value.entity_b_id}) on create set other.name = value.entity_b, 
other.location = point({latitude:toFloat(value.entity_b_lat),longitude:toFloat(value.entity_b_lng)}),
other.country = value.entity_b_iso_code
merge (c:Country {code:value.entity_b_iso_code}) on create set c.name = value.entity_b_country
merge (f)<-[:FILED]-(filer)
merge (f)-[:CONCERNS]->(other)
merge (other)-[:COUNTRY]->(c);

match (f:Filing)
set f.transactions = toInteger(f.number_transactions)
set f.amount = toFloat(f.amount_transactions)
set f.end=date(apoc.temporal.toZonedTemporal(f.end_date,"MMM dd, yyyy"))
set f.begin=date(apoc.temporal.toZonedTemporal(f.begin_date,"MMM dd, yyyy"))

merge (ben:Entity {id:f.beneficiary_bank_id})
on create set ben.name = f.beneficiary_bank, ben.location = point({latitude:toFloat(f.beneficiary_lat), longitude:toFloat(f.beneficiary_lng)})
merge (cben:Country {code:f.beneficiary_iso})
merge (ben)-[:COUNTRY]->(cben)
merge (f)-[:BENEFITS]->(ben)

merge (filer:Entity {id:f.filer_org_name_id})
on create set filer.name = f.filer_org_name, filer.location = point({latitude:toFloat(f.filer_org_lat), longitude:toFloat(f.filer_org_lng)})
merge (f)<-[:FILED]-(filer)

merge (org:Entity {id:f.originator_bank_id})
on create set org.name = f.originator_bank, org.location = point({latitude:toFloat(f.origin_lat), longitude:toFloat(f.origin_lng)})
merge (corg:Country {code:f.originator_iso})
merge (org)-[:COUNTRY]->(corg)
merge (f)-[:ORIGINATOR]->(org)
;

/*
download_transactions_map.csv

id,icij_sar_id,filer_org_name_id,filer_org_name,begin_date,end_date,originator_bank_id,originator_bank,originator_bank_country,originator_iso,beneficiary_bank_id,beneficiary_bank,beneficiary_bank_country,beneficiary_iso,number_transactions,amount_transactions
223254,3297,the-bank-of-new-york-mellon-corp,The Bank of New York Mellon Corp.,"Mar 25, 2015","Sep 25, 2015",cimb-bank-berhad,CIMB Bank Berhad,Singapore,SGP,barclays-bank-plc-london-england-gbr,Barclays Bank Plc,United Kingdom,GBR,68,5.689852347E7


download_bank_connections.csv
icij_sar_id,filer_org_name_id,filer_org_name,entity_b_id,entity_b,entity_b_country,entity_b_iso_code
4132,standard-chartered-plc,Standard Chartered Plc,habib-metropolitan-bank-limited-karachi-pakistan-pak,Habib Metropolitan Bank Limited,Pakistan,PAK
*/