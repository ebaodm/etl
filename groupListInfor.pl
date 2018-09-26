use lib '/ccicall/dc/script/Public';
use MyTools;
use DBI;
use POSIX qw/ceil/;
use threads;
#获取informatica task运行详细状态
sub gettask{
#获取INFORMATICA JOB状态
  my ($ifsv,$ifdm,$ifuser,$ifpwd,$iffloder,$ifwf,$iftask)=@_;
  my $cmd="pmcmd gettaskdetails -sv ".$ifsv." -d ".$ifdm." -u ".$ifuser." -p ".$ifpwd
           ." -folder ".$iffloder." -w ".$ifwf." ".$iftask;
  open(MSG,$cmd."|") or return {"errcode"=>1,"errmsg"=>$cmd};
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
  return {
  	'errcode'=>1,
  	'errstr'=>"执行CMD".$cmd."出错",
  } if ! defined $errcode;
  return {
  	'starttime'=>$starttime,
  	'endtime'=>$endtime,
  	'errcode'=>$errcode,
  	'errstr'=>$errstr,
  	'srcsucces'=>$srcsucces,
  	'srcfailed'=>$srcfailed,
  	'tarsucces'=>$tarsucces,
  	'tarfailed'=>$tarfailed,
  };
}

#执行informatica task
sub runtask{
#运行INFORMATICA JOB
  my ($ifsv,$ifdm,$ifuser,$ifpwd,$iffloder,$ifpara,$ifwf,$iftask)=@_;
  my $cmd="pmcmd starttask -sv ".$ifsv." -d ".$ifdm." -u ".$ifuser." -p ".$ifpwd
          ." -folder ".$iffloder.(! defined($ifpara) or $ifpara eq ""?"":" -paramfile ".$ifpara)
          ." -w ".$ifwf." -wait ".$iftask;
  open(MSG,$cmd."|") or return {"errcode"=>1,"errmsg"=>$cmd};
  my @msgstr=<MSG>;
  close(MSG);
  foreach(@msgstr){
    my $errtmp=$1 if $_=~/^ERROR:(.+)$/;
    $errtmp=~s/\r|\n//g if $errtmp;
    print "执行CMD".$cmd."出错：".$errtmp and return{
    	'errcode'=>1,
    	'errmsg'=>$errtmp,
    } if $errtmp;
  }
  my $details=&gettask($ifsv,$ifdm,$ifuser,$ifpwd,$iffloder,$ifwf,$iftask);
  #writeinfolog
  return $details;
}

sub paramfile{
	my ($folder)=@_;
	my $paramfile="/ccicall/dc/script/config/".$etlid."_param_".&currenttime;
	unlink $paramfile if -f $paramfile;
	open(CFG,">>".$paramfile) or return {"errcode"=>1,"errmsg"=>"cannot create file ".$paramfile};
	print CFG "[Global]\n";
	my $dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"}) or return {"errcode"=>DBI->err,"errmsg"=>DBI->errstr};
	my $sth=$dbh->prepare("select paramname,paramvalue from DC_INFOR_FOLDER_PARAM where folder='$folder'") or return {"errcode"=>DBI->err,"errmsg"=>DBI->errstr};
	$sth->execute();
	while(my @row=$sth->fetchrow_array){
		print CFG $row[0]."=".$row[1]."\n";
	}
	$sth->finish;
	$dbh->disconnect;
	return $paramfile;
}
our ($etlid,$folder,$maxproc)=("groupListInfor","B_TO_C_GROUP_LIST",5);
my $cfg="/ccicall/dc/script/config/conn.cfg";
&writedblog($etlid,"","",'E',$cfg."不存在",&currenttime,&currenttime,"") and exit 1 if ! -f $cfg;
open(CFG,$cfg);
our %conncfg=map{chomp;my @row=split/\=/;$row[0]=>$row[1] if $row[0]=~/^[^#]/} <CFG>;
my $inforparam=&paramfile($folder);
&writedblog($etlid,"","",$inforparam->{"errcode"},$inforparam->{"errmsg"},&currenttime,&currenttime,"") and exit 1 if $inforparam->{"errcode"};
my $tasks=&getdbresref($conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"},
	"select a.server,
       a.domain,
       a.workflow,
       a.task from dc_infor_task_list a
	 where a.folder='B_TO_C_GROUP_LIST'
	  and a.task not like 's_m_T_PA_BCP\%'");
&writedblog($etlid,"","",$tasks->{"errcode"},$tasks->{"errmsg"},&currenttime,&currenttime,"") and exit 1 if $tasks->{"errcode"};
foreach my $row (@{$tasks->{"result"}}){
	until(threads->list < $maxproc){
		foreach(threads->list(threads::all)){
			my $inforet=$_->join if $_->is_joinable;
			&writedblog($etlid,"","",$inforet->{"errcode"},$inforet->{"errstr"},&currenttime,&currenttime,"") if $inforet->{"errcode"};
    }
    sleep 1;
	}
	threads->create({scalar=>1},\&runtask,$$row[0],$$row[1],"Administrator","admin",$folder,$inforparam,$$row[2],$$row[3]);
}

until(threads->list == 0){
	foreach(threads->list(threads::all)){
		my $inforet=$_->join if $_->is_joinable;
		&writedblog($etlid,"","",$inforet->{"errcode"},$inforet->{"errstr"},&currenttime,&currenttime,"") if $inforet->{"errcode"};
	}
	sleep 1;
}

