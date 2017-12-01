use DBI;
use Data::Dumper;
my $confile="/infa/script/config/conn.cfg";
open(CONF,$confile);
my %dbconf=map{chomp;my @row=split/\=/;$row[0]=>$row[1] if $row[0]=~/^[^#]/} <CONF>;
close(CONF);
undef $confile;
my $dbh=DBI->connect("DBI:Oracle:".$dbconf{"dbname"},$dbconf{"dbuser"},$dbconf{"dbpwd"});
my $cfgsql="select DISTINCT A.MODELNAME,
	UPPER(B.FIELD_NAME),
	CASE
	WHEN B.DATATYPE = 'STRING' THEN
	'VARCHAR2(' || B.MAXLENGTH || ')'
	WHEN B.DATATYPE = 'DOUBLE' AND B.MAXLENGTH <> B.PRECISION THEN
	'NUMBER(26,6)'
	WHEN B.DATATYPE = 'BOOLEAN' THEN
	'VARCHAR2(1)'
	WHEN B.DATATYPE = 'DOUBLE' AND B.MAXLENGTH = B.PRECISION THEN
	'NUMBER(20)'
	WHEN B.DATATYPE = 'INTEGER' THEN
	'NUMBER(20)'
	ELSE
	B.DATATYPE
	END
	FROM PRODUCT_OBJ A, PRODUCT_FIELD_ATTR B
	WHERE A.PK = B.PK
	AND A.PRODUCTID = B.PRODUCTID";
my $sth=$dbh->prepare($cfgsql);
$sth->execute;
my $dbress=$sth->fetchall_arrayref;
my $tabconf;
foreach my $dbres (@{$dbress}){
	$tabconf->{$$dbres[0]}->{$$dbres[1]}=$$dbres[2];
}
$sth->finish;

foreach my $tbname (keys%{$tabconf}){
	my $dropsql="drop table $tbname";
	my $createsql="create table $tbname ( ";
	foreach my $colname (keys%{$tabconf->{$tbname}}){
		$createsql.="$colname $tabconf->{$tbname}->{$colname},";
	}
	$createsql=~s/,$//;
	$createsql.=" )";
	$dbh->do($dropsql);
	$dbh->do($createsql);
}
$dbh->disconnect;

__END__