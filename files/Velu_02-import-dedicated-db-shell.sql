/*
use PowerShell to import data to SQLPool01 (dedicated SQL )

sample DimDate
bcp 'velu.DimDate' in ./DimDate.txt -S "asaworkspace19hvmew.sql.azuresynapse.net"  -f ./DimDate.fmt -q -k -E -b 5000 -U asa.sql.admin -P '123qweASD!' -d 'SQLPool01'
bcp 'velu.DimProduct' in ./DimProduct.txt -S "asaworkspace19hvmew.sql.azuresynapse.net"  -f ./DimProduct.fmt -q -k -E -b 5000 -U asa.sql.admin -P '123qweASD!' -d 'SQLPool01'

Get-ChildItem "./data/*.txt" -File | Foreach-Object {
    $file = $_.FullName
	$table = $_.Name.Replace(".txt","")
	bcp velu.$table in $file -S "asaworkspace19hvmew.sql.azuresynapse.net"  -f -f $file.Replace("txt", "fmt") -q -k -E -b 5000 -U asa.sql.admin -P '123qweASD!' -d 'SQLPool01'
}	
	
 
 */
 TRUNCATE velu.Sale_Heap;
 GO
 insert into velu.Sale_Heap
 select * from wwi_perf.Sale_Heap;
 GO