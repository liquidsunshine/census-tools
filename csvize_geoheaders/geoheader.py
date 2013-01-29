geohead_base = {'host': "ftp.census.gov", 'cwd': "/census_2010/01-Redistricting_File--PL_94-171/"}
data_folder = "./geohead_data/"
download_and_unzip = False

# -- Stuff -- #

import ftputil, ftplib, os, zipfile, csv
structure2010 = (("FILEID",6,0,"VARCHAR"), ("STUSAB",2,6,"VARCHAR"), ("SUMLEV",3,8,"VARCHAR"), ("GEOCOMP",2,11,"VARCHAR"), ("CHARITER",3,13,"VARCHAR"), ("CIFSN",2,16,"VARCHAR"), ("LOGRECNO",7,18,"NUMERIC"), ("REGION",1,25,"VARCHAR"), ("DIVISION",1,26,"VARCHAR"), ("STATE",2,27,"VARCHAR"), ("COUNTY",3,29,"VARCHAR"), ("COUNTYCC",2,32,"VARCHAR"), ("COUNTYSC",2,34,"VARCHAR"), ("COUSUB",5,36,"VARCHAR"), ("COUSUBCC",2,41,"VARCHAR"), ("COUSUBSC",2,43,"VARCHAR"), ("PLACE",5,45,"VARCHAR"), ("PLACECC",2,50,"VARCHAR"), ("PLACESC",2,52,"VARCHAR"), ("TRACT",6,54,"VARCHAR"), ("BLKGRP",1,60,"VARCHAR"), ("BLOCK",4,61,"VARCHAR"), ("IUC",2,65,"VARCHAR"), ("CONCIT",5,67,"VARCHAR"), ("CONCITCC",2,72,"VARCHAR"), ("CONCITSC",2,74,"VARCHAR"), ("AIANHH",4,76,"VARCHAR"), ("AIANHHFP",5,80,"VARCHAR"), ("AIANHHCC",2,85,"VARCHAR"), ("AIHHTLI",1,87,"VARCHAR"), ("AITSCE",3,88,"VARCHAR"), ("AITS",5,91,"VARCHAR"), ("AITSCC",2,96,"VARCHAR"), ("TTRACT",6,98,"VARCHAR"), ("TBLKGRP",1,104,"VARCHAR"), ("ANRC",5,105,"VARCHAR"), ("ANRCCC",2,110,"VARCHAR"), ("CBSA",5,112,"VARCHAR"), ("CBSASC",2,117,"VARCHAR"), ("METDIV",5,119,"VARCHAR"), ("CSA",3,124,"VARCHAR"), ("NECTA",5,127,"VARCHAR"), ("NECTASC",2,132,"VARCHAR"), ("NECTADIV",5,134,"VARCHAR"), ("CNECTA",3,139,"VARCHAR"), ("CBSAPCI",1,142,"VARCHAR"), ("NECTAPCI",1,143,"VARCHAR"), ("UA",5,144,"VARCHAR"), ("UASC",2,149,"VARCHAR"), ("UATYPE",1,151,"VARCHAR"), ("UR",1,152,"VARCHAR"), ("CD",2,153,"VARCHAR"), ("SLDU",3,155,"VARCHAR"), ("SLDL",3,158,"VARCHAR"), ("VTD",6,161,"VARCHAR"), ("VTDI",1,167,"VARCHAR"), ("RESERVE2",3,168,"VARCHAR"), ("ZCTA5",5,171,"VARCHAR"), ("SUBMCD",5,176,"VARCHAR"), ("SUBMCDCC",2,181,"VARCHAR"), ("SDELM",5,183,"VARCHAR"), ("SDSEC",5,188,"VARCHAR"), ("SDUNI",5,193,"VARCHAR"), ("AREALAND",14,198,"NUMERIC"), ("AREAWATR",14,212,"NUMERIC"), ("NAME",90,226,"VARCHAR"), ("FUNCSTAT",1,316,"VARCHAR"), ("GCUNI",1,317,"VARCHAR"), ("POP100",9,318,"NUMERIC"), ("HU100",9,327,"NUMERIC"), ("INTPTLAT",11,336,"VARCHAR"), ("INTPTLON",12,347,"VARCHAR"), ("LSADC",2,359,"VARCHAR"), ("PARTFLAG",1,361,"VARCHAR"), ("RESERVE3",6,362,"VARCHAR"), ("UGA",5,368,"VARCHAR"), ("STATENS",8,373,"VARCHAR"), ("COUNTYNS",8,381,"VARCHAR"), ("COUSUBNS",8,389,"VARCHAR"), ("PLACENS",8,397,"VARCHAR"), ("CONCITNS",8,405,"VARCHAR"), ("AIANHHNS",8,413,"VARCHAR"), ("AITSNS",8,421,"VARCHAR"), ("ANRCNS",8,429,"VARCHAR"), ("SUBMCDNS",8,437,"VARCHAR"), ("CD113",2,445,"VARCHAR"), ("CD114",2,447,"VARCHAR"), ("CD115",2,449,"VARCHAR"), ("SLDU2",3,451,"VARCHAR"), ("SLDU3",3,454,"VARCHAR"), ("SLDU4",3,457,"VARCHAR"), ("SLDL2",3,460,"VARCHAR"), ("SLDL3",3,463,"VARCHAR"), ("SLDL4",3,466,"VARCHAR"), ("AIANHHSC",2,469,"VARCHAR"), ("CSASC",2,471,"VARCHAR"), ("CNECTASC",2,473,"VARCHAR"), ("MEMI",1,475,"VARCHAR"), ("NMEMI",1,476,"VARCHAR"), ("PUMA",5,477,"VARCHAR"), ("RESERVED",18,482,"VARCHAR"))

def parse_record(rec_str, structure = structure2010, return_type = "dict"):
  # Takes a string that conforms to "structure" and returns
  # a dict version of it
  
  if return_type == "dict":
    record = {}
    for (col_id, length, start, var_type) in structure:
      record[col_id] = rec_str[start:start+length].rstrip().lstrip()
  elif return_type == "list":
    record = []
    for (col_id, length, start, var_type) in structure:
      record.append(rec_str[start:start+length].rstrip().lstrip())
  else:
    return None

  return record
  
def parse_file(filename, structure = structure2010, return_type = "dict"):
#  record_list = []
  
  with open(filename) as f:
    for rec_str in f:
      yield parse_record( rec_str, structure, return_type )
    
#  return record_list

if __name__ == '__main__':
    
  # Download some files from the login directory.
  host = ftputil.FTPHost(geohead_base["host"], 'anonymous', 'anonymous')

  if not os.path.isdir(data_folder):
    os.makedirs(data_folder)

  if download_and_unzip:
    for folder,zzzzz,fnames in host.walk(geohead_base["cwd"]):
      for f in fnames:
	if f[2:] == "2010.pl.zip":
	  print "Downloading {0}".format(f)
	  host.chdir(folder)
	  host.download_if_newer(f, data_folder+f, 'b')
	  
	  # Once it's downloaded, unzip the target file.
	  with zipfile.ZipFile(data_folder+f, 'r') as state_zip:
	    for zf in state_zip.infolist():
	      if zf.filename[2:] == "geo2010.pl":
		print "Extracting {0}".format(zf.filename)
		state_zip.extract(zf, data_folder)

  with open(data_folder+'all_geoheaders.csv', 'wb') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
    for f in os.listdir(data_folder):
      if f[2:] == "geo2010.pl":
	record_list = parse_file(data_folder+f, return_type="list")
	print "Writing {0} to CSV".format(f)
	for r in record_list:
	  writer.writerow(r)  
	del record_list