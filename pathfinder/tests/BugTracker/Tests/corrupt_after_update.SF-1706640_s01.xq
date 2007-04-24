for $entity in doc("observations_consumer01.xml")//observation/info/*
for $cluster in doc("clusters_consumer01.xml")//clusters/*
where 
      name($cluster) = name($entity)
and (every $value in $entity/* satisfies 
	fn:exists($cluster/*[name() = name($value)][data(.)=data($value)]))
return 
	if (exists($entity/cluster[@id = $cluster/@id])) then
		()
	else
		do insert element { "cluster" } { $cluster/@id } into $entity
