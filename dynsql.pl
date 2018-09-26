use lib '/ccicall/dc/script/Public';
use MyTools;
use DBI;
use strict;
$|=1;
my ($etlid,$fullflag,$orgcode,$riskcode)=@ARGV;
#初始数据库连接
my $conncfg="/ccicall/dc/script/config/conn.cfg";
open(CFG,$conncfg) or die $conncfg."not exists";#log
my %conncfg=map{chomp;my @row=split/\=/;$row[0]=>$row[1] if $row[0]=~/^[^#]/} <CFG>;
my $startime=&currenttime;
#获取数据库用户连接信息
my $dbresref=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	'select upper(TNS_NAME),UPPER(DB_USER),DB_PASS from DB_USER');
if($dbresref->{"errcode"}){
	&writedblog($etlid,$orgcode,$riskcode,$dbresref->{"errcode"},$dbresref->{"errmsg"},$startime,$startime,"");
	exit 1;
}
my $parausers;
if (@{$dbresref->{"result"}} == 0){
	&writedblog($etlid,$orgcode,$riskcode,'E',"数据库用户配置为空",$startime,$startime,"");
	exit 1; 
}
foreach my $row (@{$dbresref->{"result"}}){
	$parausers->{$$row[0]}->{$$row[1]}->{"PASSWORD"}=$$row[2];
}
undef $dbresref;
#HINT结果集
my $hintdbres=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"SELECT A.HINT_SQL,A.HINT_SQL_INC,A.DISTINCT_FLAG FROM TASK_LIST A WHERE A.ETL_ID= '".$etlid."'");
if($hintdbres->{"errcode"}){
	&writedblog($etlid,$orgcode,$riskcode,$hintdbres->{"errcode"},$hintdbres->{"errmsg"},$startime,$startime,"");
	exit 1;
}
if (@{$hintdbres->{"result"}} == 0){
	&writedblog($etlid,$orgcode,$riskcode,'E',"任务为空",$startime,$startime,"");
	exit 1; 
}

#表清单结果集
my $tblistdbres=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"SELECT TABLE_ID,TABLE_ALIAS,TARGET_TAB,TABLE_TYPE,upper(DB_USER),
	upper(TNS_NAME) FROM TABLE_LIST WHERE ETL_ID = '".$etlid."'");
if($tblistdbres->{"errcode"}){
	&writedblog($etlid,$orgcode,$riskcode,$tblistdbres->{"errcode"},$tblistdbres->{"errmsg"},$startime,$startime,"");
	exit 1;
}
if (@{$tblistdbres->{"result"}} == 0){
	&writedblog($etlid,$orgcode,$riskcode,'E',"任务依赖表为空",$startime,$startime,"");
	exit 1; 
}

my $tblist;
my $footsql;
if(defined ${$hintdbres->{"result"}}[0][2] and ${$hintdbres->{"result"}}[0][2] eq "1"){
	$footsql="select distinct ";
}else{
	$footsql="select ";
}
my ($headsql,$insertown,$inserttns,$mastertbid);
foreach my $row (@{$tblistdbres->{"result"}}){
	$tblist->{$$row[0]}->{"alias"}=$$row[1];
	$tblist->{$$row[0]}->{"tbname"}=$$row[2];
	$tblist->{$$row[0]}->{"tbtype"}=$$row[3];
	$tblist->{$$row[0]}->{"owner"}=$$row[4];
	$tblist->{$$row[0]}->{"tns"}=$$row[5];
	if($$row[3]==1){
		$insertown=$$row[4];
		$inserttns=$$row[5];
		if($fullflag==1){
			$headsql.="insert ".${$hintdbres->{"result"}}[0][0]." into ".$$row[4].".".$$row[2]."(\n";
		}else{
			$headsql.="insert ".${$hintdbres->{"result"}}[0][1]." into ".$$row[4].".".$$row[2]."(\n";
		}
	}
	if($$row[3]==2){
		$mastertbid=$$row[0];
	}
}
undef $tblistdbres;
undef $hintdbres;
#字段结果集
my $dbresref=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"select	upper(a.source_column),
	upper(a.target_column),
	a.value_mapping,
	A.TARGET_TABLE_ID,
	A.SOURCE_TABLE_ID
	from COLUMNS_MAPPING A
	WHERE A.ETL_ID = '".$etlid."'");
if($dbresref->{"errcode"}){
	&writedblog($etlid,$orgcode,$riskcode,$dbresref->{"errcode"},$dbresref->{"errmsg"},$startime,$startime,"");
	exit 1;
}
if (@{$dbresref->{"result"}} == 0){
	&writedblog($etlid,$orgcode,$riskcode,'E',"任务字段为空",$startime,$startime,"");
	exit 1; 
}

foreach my $row (@{$dbresref->{"result"}}){
	$headsql.=$$row[1].",\n";
	#拼接select字段 select a.col,..
	$footsql.=$tblist->{$$row[4]}->{"alias"}.".".$$row[0].",\n" if ! defined $$row[2];
	$footsql.=$$row[2].",\n" if defined $$row[2];
}
undef $dbresref;
$headsql=~s/,\n$//;
$footsql=~s/,\n$//;
$headsql.=")\n";

#表关系结果集
my $dbresreftbrl=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"select UPPER(A.TAB_RL_ID),
	UPPER(A.RL_TYPE),
	A.MASTER_TABLE_ID,
	A.SUB_TABLE_ID
	from TABLE_RL A
	WHERE A.ETL_ID = '".$etlid."'  ORDER BY TAB_RL_ID");
if($dbresreftbrl->{"errcode"}){
	&writedblog($etlid,$orgcode,$riskcode,$dbresreftbrl->{"errcode"},$dbresreftbrl->{"errmsg"},$startime,$startime,"");
	exit 1;
}
my $tbexists;
if(@{$dbresreftbrl->{"result"}} == 0){
	if($tblist->{$mastertbid}->{"tbname"}=~/^\s{0,}\(/){
		$footsql.=" FROM ".$tblist->{$mastertbid}->{"tbname"}." ".$tblist->{$mastertbid}->{"alias"};
	}else{
		$footsql.=" FROM ".$tblist->{$mastertbid}->{"owner"}.".".$tblist->{$mastertbid}->{"tbname"}." ".$tblist->{$mastertbid}->{"alias"};
	}
	$tbexists->{$mastertbid}=1;
}else{
	$footsql.=" FROM ";
}

#拼接on
foreach my $row (@{$dbresreftbrl->{"result"}}){
	if(exists($tbexists->{$$row[2]})){
		if($tblist->{$$row[3]}->{"tbname"}=~/^\s{0,}\(/){
			$footsql.=" ".$$row[1]." ".$tblist->{$$row[3]}->{"tbname"}." ".$tblist->{$$row[3]}->{"alias"};
		}else{
			$footsql.=" ".$$row[1]." ".$tblist->{$$row[3]}->{"owner"}.".".$tblist->{$$row[3]}->{"tbname"}." ".$tblist->{$$row[3]}->{"alias"};
		}
		$tbexists->{$$row[3]}=1;
	}elsif(exists($tbexists->{$$row[3]})){
		if($tblist->{$$row[2]}->{"tbname"}=~/^\s{0,}\(/){
			$footsql.=" ".$$row[1]." ".$tblist->{$$row[2]}->{"tbname"}." ".$tblist->{$$row[2]}->{"alias"};
		}else{
			$footsql.=" ".$$row[1]." ".$tblist->{$$row[2]}->{"owner"}.".".$tblist->{$$row[2]}->{"tbname"}." ".$tblist->{$$row[2]}->{"alias"};
		}
		$tbexists->{$$row[2]}=1;
	}else{
		if($tblist->{$$row[2]}->{"tbname"}=~/^\s{0,}\(/){
			$footsql.=" ".$tblist->{$$row[2]}->{"tbname"}." ".$tblist->{$$row[2]}->{"alias"}." ".$$row[1]." ";
		}else{
			$footsql.=" ".$tblist->{$$row[2]}->{"owner"}.".".$tblist->{$$row[2]}->{"tbname"}." ".$tblist->{$$row[2]}->{"alias"}." ".$$row[1]." ";
		}
		if($tblist->{$$row[3]}->{"tbname"}=~/^\s{0,}\(/){
			$footsql.=" ".$tblist->{$$row[3]}->{"tbname"}." ".$tblist->{$$row[3]}->{"alias"};
		}else{
			$footsql.=" ".$tblist->{$$row[3]}->{"owner"}.".".$tblist->{$$row[3]}->{"tbname"}." ".$tblist->{$$row[3]}->{"alias"};
		}
		$tbexists->{$$row[2]}=1;
		$tbexists->{$$row[3]}=1;
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
	if($dbresref1->{"errcode"}){
		&writedblog($etlid,$orgcode,$riskcode,$dbresref1->{"errcode"},$dbresref1->{"errmsg"},$startime,$startime,"");
		exit 1;
	}
	my $flag=0;
	foreach my $colrl (@{$dbresref1->{"result"}}){
		if($flag == 0){
			$footsql.="\n on ";
		}else {
			$footsql.="\n and ";
		}
		$footsql.=" ".$tblist->{$$row[2]}->{"alias"}.".".$$colrl[0]." ".$$colrl[2]." ".$tblist->{$$row[3]}->{"alias"}.".".$$colrl[1] if ! defined $$colrl[3] and ! defined $$colrl[4];
		$footsql.=" ".$tblist->{$$row[3]}->{"alias"}.".".$$colrl[1]." ".$$colrl[2]." ".$$colrl[4] if ! defined $$colrl[3] and defined $$colrl[4];
		$footsql.=" ".$tblist->{$$row[2]}->{"alias"}.".".$$colrl[0]." ".$$colrl[2]." ".$$colrl[3] if defined $$colrl[3] and ! defined $$colrl[4];
		$footsql.=" ".$$colrl[3]." ".$$colrl[2]." ".$$colrl[4] if defined $$colrl[3] and defined $$colrl[4];
		$flag++;
	}
}
undef $dbresreftbrl;

my $wheresql.=" where 1=1";
#where
my $dbresref=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"select A.CONDITION_TABLE_ID,
	A.CONDITION_COLUMN,
	A.CONDITION_FORMULA,
	A.CONDITION_VALUE,
	A.COND_COL_EXPR
	FROM TABLE_CONDITION A
	WHERE A.ETL_ID = '".$etlid."'");
if($dbresref->{"errcode"}){
	&writedblog($etlid,$orgcode,$riskcode,$dbresref->{"errcode"},$dbresref->{"errmsg"},$startime,$startime,"");
	exit 1;
}
foreach my $cond (@{$dbresref->{"result"}}){
	if(defined $$cond[4]){
		$wheresql.=" and ".$$cond[4]." ".$$cond[2]." ".$$cond[3];
	}else{
		$wheresql.=" and ".$tblist->{$$cond[0]}->{"alias"}.".".$$cond[1]." ".$$cond[2]." ".$$cond[3];
	}
}
undef $dbresref;
my $dbh=&getdbh($inserttns,$insertown,$parausers->{$inserttns}->{$insertown}->{"PASSWORD"});
if($dbh->{"errcode"}){
	&writedblog($etlid,$orgcode,$riskcode,$dbh->{"errcode"},$dbh->{"errmsg"},$startime,$startime,"");
	exit 1;
}
print $headsql.$footsql.$wheresql;
my $res=$dbh->{"dbh"}->do($headsql.$footsql.$wheresql);
if(DBI->err){
	&writedblog($etlid,$orgcode,$riskcode,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
&writedblog($etlid,$orgcode,$riskcode,"C","",$startime,&currenttime,$res);
open(DEBUGLIST,'>>/ccicall/dc/script/debuglist.txt');
print DEBUGLIST "\n============================================\n" if $dbh->{"dbh"}->err;
print DEBUGLIST "etlid=".$etlid." error\nerrms:".$dbh->{"dbh"}->errstr and exit 2 if $dbh->{"dbh"}->err;
$dbh->{"dbh"}->disconnect();
exit 0;