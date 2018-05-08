use strict;
use lib '/ccicall/dc/script/Public';
use MyTools;
use threads;
use DBI;

my $conncfg="/ccicall/dc/script/config/conn.cfg";
open(CFG,$conncfg) or die $conncfg."not exists";#log
my %conncfg=map{chomp;my @row=split/\=/;$row[0]=>$row[1] if $row[0]=~/^[^#]/} <CFG>;

my $dbresref=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"SELECT BUSINESSCODE,PRODUCTID FROM PRODUCT_PRODUCT WHERE BUSINESSCODE LIKE 'D%'");

my $loopnum=500;#单线程批量单数
my $maxproc=6;#最大线程数

sub createjson{
	my ($productid,$loopid)=@_;
	system("perl /ccicall/dc/script/pajson_bak6.pl $productid $loopid");
	my $dbh=DBI->connect("DBI:Oracle:pccictst9","dc_admin","dc_admin");
	$dbh->do("drop table dc_json_driver_$loopid");
}
my $thrcount=0;
foreach my $row (@{$dbresref->{"result"}}){
	my $dbh=DBI->connect("DBI:Oracle:pccictst9","dc_admin","dc_admin");
	print DBI->errstr if DBI->err;
	my $sth=$dbh->prepare("select count(1) from dc_admin.dc_json_policy_list a where a.riskcode='".$$row[0]."'");
	print DBI->errstr if DBI->err;
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
		$sth=$dbh->prepare("create table dc_json_driver_$loopid as select policyno driver_key,1 lv from dc_json_policy_list where rownum<=$loopnum and riskcode='$$row[0]'");
		$sth->execute;
		$sth->finish;
		$dbh->do("delete from dc_json_policy_list a where exists(select 1 from dc_json_driver_$loopid b where a.policyno=b.driver_key and b.lv=1)");
		$dbh->disconnect;
		threads->create(\&createjson,$$row[1],$loopid);
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
