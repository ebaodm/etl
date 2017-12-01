package MyTools;
use strict;
use Exporter;
use DBI;

our @ISA = qw(Exporter);
our @EXPORT=qw(getdbresref test getdbres getdbh);

#获取informatica task运行详细状态
sub gettask(){
  my ($ifsv,$ifdm,$ifuser,$ifpwd,$iffloder,$ifwf,$iftask)=@_;
  my $cmd="pmcmd gettaskdetails -sv ".$ifsv." -d ".$ifdm." -u ".$ifuser." -p ".$ifpwd
           ." -folder ".$iffloder." -w ".$ifwf." ".$iftask;
  my $ret;
  open(MSG,$cmd."|");
  my @msgstr=<MSG>;
  my $msgtmp=join("",@msgstr);
  close(MSG);
  my ($starttime,$endtime,$errcode,$errstr,$srcsucces,$srcfailed,$tarsucces,$tarfailed);
  $starttime=$1 if $msgtmp=~/Start time\: \[([^\]]+)/;
  $endtime=$1 if $msgtmp=~/End time\: \[([^\]]+)/;
  $errcode=$1 if $msgtmp=~/Task run error code\: \[([^\]]+)/;
  $errstr=$1 if $msgtmp=~/First error message\: \[([^\]]+)/;
  $errstr=~s/\n|\r//g;
  $srcsucces=$1 if $msgtmp=~/Source success rows\: \[([^\]]+)/;
  $srcfailed=$1 if $msgtmp=~/Source failed rows\: \[([^\]]+)/;
  $tarsucces=$1 if $msgtmp=~/Target success rows\: \[([^\]]+)/;
  $tarfailed=$1 if $msgtmp=~/Target failed rows\: \[([^\]]+)/;
  $ret={
  	"starttime"=>"",
  	"endtime"=>"",
  	"errcode"=>1,
  	"errstr"=>"cmd执行出错".$cmd,
  	"srcsucces"=>"",
  	"srcfailed"=>"",
  	"tarsucces"=>"",
  	"tarfailed"=>"",
  } and return $ret if ! defined $errcode;
  $ret={
  	"starttime"=>$starttime,
  	"endtime"=>$endtime,
  	"errcode"=>$errcode,
  	"errstr"=>$errstr,
  	"srcsucces"=>$srcsucces,
  	"srcfailed"=>$srcfailed,
  	"tarsucces"=>$tarsucces,
  	"tarfailed"=>$tarfailed,
  };
  return $ret;
}

sub getdbh{
	my $ret;
	my ($dbname,$dbuser,$dbpwd)=@_;
	my $dbh=DBI->connect("DBI:Oracle:".$dbname,$dbuser,$dbpwd);
	$ret={
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr,
		"dbh"=>$dbh,
	};
	return $ret;
}
#返回引用 数据库查询
sub getdbresref{
	die "参数为4" if @_ ne 4;
	my ($dbname,$dbuser,$dbpwd,$sql)=@_;
	my $ret;
	my $dbh=&getdbh($dbname,$dbuser,$dbpwd);
	$ret={
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr,
		"dbh"=>undef,
	} and return $ret if $dbh->{"errcode"};
	my $sth=$dbh->{"dbh"}->prepare($sql);
	$ret={
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr,
		"dbh"=>undef,
	} and return (DBI->err,DBI->errstr,undef) if DBI->err;
	$sth->execute();
	my $result=$sth->fetchall_arrayref();
	$dbh->{"dbh"}->disconnect();
	$ret={
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr,
		"result"=>$result,
	};
	return $ret;
}

#返回数据库查询单行单列
sub getdbres{
	die "参数为4" if @_ ne 4;
	my ($dbname,$dbuser,$dbpwd,$sql)=@_;
	my $ret;
	my $dbh=&getdbh($dbname,$dbuser,$dbpwd);
	$ret={
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr,
		"result"=>undef,
	} and return $ret if $dbh->{"errcode"};
	my $sth=$dbh->{"dbh"}->prepare($sql);
	$ret={
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr,
		"result"=>undef,
	} and return $ret if DBI->err;
	$sth->execute();
	my $result=$sth->fetchrow_array();
	$ret={
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr,
		"result"=>$result,
	};
	return $ret;
}

sub test(){
	print "use ok";
}
1;
