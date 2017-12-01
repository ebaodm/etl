use lib '/infa/script/Public';
use MyTools;
use DBI;
use Data::Dumper;
use threads;
use strict;
$|=1;

my ($etlid,$fullflag)=@ARGV;
#初始数据库连接
my $conncfg="/infa/script/config/conn.cfg";
open(CFG,$conncfg) or die $conncfg."not exists";
my %conncfg=map{chomp;my @row=split/\=/;$row[0]=>$row[1] if $row[0]=~/^[^#]/} <CFG>;

#获取数据库用户连接信息
my $parausers;
my $dbresref=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	'select upper(TNS_NAME),UPPER(DB_USER),DB_PASS from DB_USER');
die $dbresref->{"errstr"} if $dbresref->{"err"};
foreach my $row (@{$dbresref->{"result"}}){
	$parausers->{$$row[0]}->{$$row[1]}->{"PASSWORD"}=$$row[2];
}
#表清单结果集
my $tblistdbres=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"SELECT TABLE_ID, TABLE_ALIAS FROM TABLE_LIST WHERE ETL_ID = '".$etlid."'");

my $masterref=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"SELECT UPPER(TARGET_TAB), TABLE_ALIAS,UPPER(DB_USER) FROM TABLE_LIST WHERE TABLE_TYPE='2' AND ETL_ID = '".$etlid."'");
my $tbalias;
foreach my $row (@{$tblistdbres->{"result"}}){
	$tbalias->{$$row[0]}=$$row[1];
}
undef $tblistdbres;

#HINT结果集
my $hintdbres=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"SELECT A.HINT_SQL,A.HINT_SQL_INC FROM TASK_LIST A WHERE A.ETL_ID= '".$etlid."'");

#表关系结果集
my $dbresreftbrl=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"select UPPER(A.TAB_RL_ID),
	UPPER(A.MASTER_DB_USER),
	UPPER(A.MASTER_TARGET_TAB),
	UPPER(A.SUB_DB_USER),
	UPPER(A.SUB_TARGET_TAB),
	UPPER(A.RL_TYPE),
	A.MASTER_TABLE_ID,
	A.SUB_TABLE_ID
	from TABLE_RL A
	WHERE A.ETL_ID = '".$etlid."'");

#字段映射结果集
my $dbresref=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"select upper(a.source_tns),
	upper(a.source_db_user),
	upper(a.source_table),
	upper(a.source_column),
	upper(a.target_tns),
	upper(a.target_db_user),
	upper(a.target_table),
	upper(a.target_column),
	a.value_mapping,
	A.TARGET_TABLE_ID,
	A.SOURCE_TABLE_ID
	from COLUMNS_MAPPING A
	WHERE A.ETL_ID = '".$etlid."'");
my ($headsql,$footsql);
#拼接HINT
$footsql.="select ";

print $etlid."任务不存在dc_admin.columns_mapping\n" and exit 1 if @{$dbresref->{"result"}} eq 0;
my ($mastertb,$insertown,$inserttns,$masteralias);
#拼接映射
foreach my $row (@{$dbresref->{"result"}}){
	if(! defined $headsql){
		#拼接insert头 insert /*+hint*/ into targettb
		$headsql.="insert ".${$hintdbres->{"result"}}[0][0]." into ".$$row[5].".".$$row[6]."(\n" if $fullflag==1;
		$headsql.="insert ".${$hintdbres->{"result"}}[0][1]." into ".$$row[5].".".$$row[6]."(\n" if $fullflag==0;
		my $dbh=&getdbh($$row[4],$$row[5],$parausers->{$$row[4]}->{$$row[5]}->{"PASSWORD"});
		#my $res=$dbh->{"dbh"}->do("truncate table ".$$row[5].".".$$row[6]);
		print $dbh->{"dbh"}->errstr if $dbh->{"dbh"}->err;
		$dbh->{"dbh"}->disconnect();
	}
	#headsql = insert /*+hint*/ into targettb(col,..)
	$headsql.=$$row[7].",\n";
	#拼接select字段 select a.col,..
	$footsql.=$tbalias->{$$row[10]}.".".$$row[3].",\n" if ! defined $$row[8];
	$footsql.=$$row[8].",\n" if defined $$row[8];
	$mastertb=$$row[1].".".$$row[2] and $masteralias=$tbalias->{$$row[10]} if ! defined $mastertb and @{$dbresreftbrl->{"result"}} == 0 and defined $$row[1] and defined $$row[2];
	$insertown=$$row[5] if ! defined $insertown;
	$inserttns=$$row[4] if ! defined $inserttns;
}
$headsql=~s/,$//;
$footsql=~s/,$//;
$headsql.=") ";
my $tblist;
#$mastertb ? $footsql.=" FROM ".$mastertb : $footsql.=" FROM ";
#拼接from字段 footsql= select a.col,.. from source1 a
$mastertb=${$masterref->{"result"}}[0][2].".".${$masterref->{"result"}}[0][0] if ! defined $mastertb;
$masteralias=${$masterref->{"result"}}[0][1] if ! defined $masteralias;
if(@{$dbresreftbrl->{"result"}} == 0){
	$footsql.=" FROM ".$mastertb." ".$masteralias;
	$tblist->{$mastertb}=1;
}else{
	$footsql.=" FROM ";
}
undef $masterref;
#拼接on
foreach my $row (@{$dbresreftbrl->{"result"}}){ #table_rl
	if(exists($tblist->{$$row[1].".".$$row[2]})){
		$footsql.=" ".$$row[5]." ".$$row[3].".".$$row[4]." ".$tbalias->{$$row[7]}." ";
		$tblist->{$$row[3].".".$$row[4]}=1;
	}elsif(exists($tblist->{$$row[3].".".$$row[4]})){
		$footsql.=" ".$$row[5]." ".$$row[1].".".$$row[2]." ".$tbalias->{$$row[6]}." ";
		$tblist->{$$row[1].".".$$row[2]}=1;
	}else{
		$footsql.=" ".$$row[1].".".$$row[2]." ".$tbalias->{$$row[6]}." ".$$row[5]." ".$$row[3].".".$$row[4]." ".$tbalias->{$$row[7]}." ";
		$tblist->{$$row[1].".".$$row[2]}=1;
		$tblist->{$$row[3].".".$$row[4]}=1;
	}
#表关联关系的字段结果集
	my $dbresref1=&getdbresref($conncfg{"dbname"},
		$conncfg{"dbuser"},
		$conncfg{"dbpwd"},
		"select UPPER(A.MASTER_COLUMN),
		UPPER(A.SUB_COLUMN),
		UPPER(A.RL_TYPE),
		A.MASTER_VALUE_RL,
		A.SUB_VALUE_RL
		from COLUMN_RL A
		WHERE A.TAB_RL_ID = '".$$row[0]."'");
	my $flag=0;
	foreach my $row1  (@{$dbresref1->{"result"}}){
		#拼接多个关联字段为and，拼接单个为on
		if($flag == 0){
			$footsql.="\n on ";
		}else {
			$footsql.="\n and ";
		}
		$footsql.=" ".$tbalias->{$$row[6]}.".".$$row1[0]." ".$$row1[2]." ".$tbalias->{$$row[7]}.".".$$row1[1] if ! defined $$row1[3] and ! defined $$row1[4];
		$footsql.=" ".$tbalias->{$$row[7]}.".".$$row1[1]." ".$$row1[2]." ".$$row1[4] if ! defined $$row1[3] and defined $$row1[4];
		$footsql.=" ".$tbalias->{$$row[6]}.".".$$row1[0]." ".$$row1[2]." ".$$row1[3] if defined $$row1[3] and ! defined $$row1[4];
		$flag++;
	}
}

#test;
$footsql.="\n where 1=1 \nand rownum<1";
my $dbh=&getdbh($inserttns,$insertown,$parausers->{$inserttns}->{$insertown}->{"PASSWORD"});
my $res=$dbh->{"dbh"}->do($headsql.$footsql);
print $headsql.$footsql."\n";
open(DEBUGLIST,'>>/infa/script/debuglist.txt');
print DEBUGLIST "\n============================================\n" if $dbh->{"dbh"}->err;
print DEBUGLIST "etlid=".$etlid." error\nerrms:".$dbh->{"dbh"}->errstr and exit 2 if $dbh->{"dbh"}->err;
$dbh->{"dbh"}->disconnect();
