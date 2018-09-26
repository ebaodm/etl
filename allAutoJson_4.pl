use strict;
use lib '/ccicall/dc/script/Public';
use MyTools;
use threads;
use DBI;
my $machineseq=40;
my $listtable="dc_json_policy_list_".$machineseq;
my $conncfg="/ccicall/dc/script/config/conn.cfg";
open(CFG,$conncfg) or die $conncfg."not exists";#log
my %conncfg=map{chomp;my @row=split/\=/;$row[0]=>$row[1] if $row[0]=~/^[^#]/} <CFG>;
my $etlid="allAutoJson.pl";
my $dbresref=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"SELECT BUSINESSCODE, PRODUCTID
  FROM PRODUCT_PRODUCT a
  where exists(select 1 from dc_json_policy_list_".$machineseq." b where a.businesscode=b.RISKCODE)");

my $loopnum=300;#单线程批量单数
my $maxproc=6;#最大线程数
if( @ARGV !=2 and @ARGV != 1 ){
	&writedblog($etlid,"","","E","参数错误",&currenttime,&currenttime,"");
	exit 1;
}
my $tablespace=$ARGV[0] if @ARGV == 1;
my $exppath=$ARGV[1] if @ARGV == 2;
sub createjson{
	my ($productid,$loopdriver,$tablespace,$exppath)=@_;
	system("perl /ccicall/dc/script/pajson_bak8.pl $productid $loopdriver $tablespace $exppath");
	my $dbh=DBI->connect("DBI:Oracle:pccictst9","dc_admin","dc_admin");
	&writedblog($etlid,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") if DBI->err;
	$dbh->do("drop table dc_json_driver_$loopdriver");
	&writedblog($etlid,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") if DBI->err;
	$dbh->disconnect;
}
my $thrcount=0;
foreach my $row (@{$dbresref->{"result"}}){
	my $dbh=DBI->connect("DBI:Oracle:pccictst9","dc_admin","dc_admin");
	&writedblog($etlid,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") if DBI->err;
	my $sth=$dbh->prepare("select count(1) from dc_admin.$listtable a where a.riskcode='".$$row[0]."'");
	&writedblog($etlid,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") if DBI->err;
	$sth->execute();
	my $dbrowcount=$sth->fetchrow_array();
	$sth->finish();
	$dbh->disconnect();
	print $$row[0]."=".$dbrowcount."\n";
	next if $dbrowcount==0;
	my $loopid=0;
	for(my $count=0;$count<$dbrowcount;$count+=$loopnum){
		print "count=$count,dbrowcount=$dbrowcount\n";
		$dbh=DBI->connect("DBI:Oracle:pccictst9","dc_admin","dc_admin");
		my $loopdriver=$machineseq."_".$loopid;
		$sth=$dbh->prepare("create table dc_json_driver_$loopdriver as select policyno driver_key,1 lv from $listtable where rownum<=$loopnum and riskcode='$$row[0]'");
		&writedblog($etlid,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") if DBI->err;
		$sth->execute;
		&writedblog($etlid,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") if DBI->err;
		$sth->finish;
		$dbh->do("delete from $listtable a where exists(select 1 from dc_json_driver_$loopdriver b where a.policyno=b.driver_key and b.lv=1)");
		&writedblog($etlid,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") if DBI->err;
		$dbh->disconnect;
		threads->create(\&createjson,$$row[1],$loopdriver,$tablespace,$exppath);
		do{
			foreach(threads->list(threads::all)){
				$_->join if $_->is_joinable;
			}
			sleep 0.1;
		}until(threads->list<$maxproc);
		$loopid++;
	}
	#单产品结束等待线程全部完成
	do{
			foreach(threads->list(threads::all)){
				$_->join if $_->is_joinable;
			}
			sleep 0.1;
	}until(threads->list==0);
}
