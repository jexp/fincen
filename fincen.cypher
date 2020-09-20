create constraint on (c:Country) assert c.code is unique;
create constraint on (e:Entity) assert e.id is unique;
create constraint on (f:Filing) assert f.id is unique;
create index on :Filing(sar_id);

call apoc.load.json("file:///fincen/countries.json") yield value
merge (c:Country {code:value.iso3}) set c.name = value.name, c.tld = value.iso2, c.location = point({latitude:toFloat(value.lat), longitude:toFloat(value.lng)})
with * where not value.exist_transaction is null set c:ExistTransactions;

call apoc.load.json("file:///fincen/sar-data.json") yield value
merge (s:Filing {id:value.id}) set s += value;

call apoc.load.json("file:///fincen/sar-details.json") yield value
match (f:Filing {sar_id:value.sar_id})
merge (filer:Entity {id:value.filer_org_name_id}) on create set filer.name = value.filer_org_name, 
filer.location = point({latitude:toFloat(value.filer_org_lat),longitude:toFloat(value.filer_org_lng)})
merge (other:Entity {id:value.entity_b_id}) on create set other.name = value.entity_b, 
other.location = point({latitude:toFloat(value.entity_b_lat),longitude:toFloat(value.entity_b_lng)}),
other.country = value.entity_b_iso_code
merge (c:Country {code:value.entity_b_iso_code})
merge (f)<-[:FILED]-(filer)
merge (f)-[:CONCERNS]->(other)
merge (other)-[:COUNTRY]->(c);

match (f:Filing)
set f.end=datetime(f.end_date_format)
set f.begin=datetime(f.begin_date_format)

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
sar-data.json
{
  "end_date": "Sep 25, 2015",
  "amount": 56898523.47,
  "beneficiary_iso": "GBR",
  "beneficiary_lng": "-2",
  "begin_date": "Mar 25, 2015",
  "originator_bank": "CIMB Bank Berhad",
  "beneficiary_lat": "54",
  "begin_date_format": "2015-03-25T00:00:00Z",
  "end_date_format": "2015-09-25T00:00:00Z",
  "originator_iso": "SGP",
  "beneficiary_bank_id": "barclays-bank-plc-london-england-gbr",
  "origin_lat": "1.3667",
  "number": 68,
  "filer_org_name": "The Bank of New York Mellon Corp.",
  "originator_bank_country": "Singapore",
  "filer_org_name_id": "the-bank-of-new-york-mellon-corp",
  "beneficiary_bank": "Barclays Bank Plc",
  "beneficiary_bank_country": "United Kingdom",
  "origin_lng": "103.8",
  "id": "223254",
  "originator_bank_id": "cimb-bank-berhad",
  "sar_id": "3297"
}
*/
