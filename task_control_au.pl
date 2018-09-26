use lib '/ccicall/dc/script/Public';
use MyTools;
use DBI;
use Data::Dumper;
#use threads;
use strict;
$|=1;

#根据任务号获取目标表
sub gettartab{
	my ($etlid,$dbname,$dbuser,$dbpwd)=@_;
	return {
		"errcode"=>1,
		"errmsg"=>"参数错误"
	} if @_ != 4;
	my $dbresref=&getdbres($dbname,
		$dbuser,
		$dbpwd,
		"select upper(a.target_tab) from table_list a where a.etl_id='".$etlid."' and a.table_type='1'");
	return {
		"errcode"=>$dbresref->{"errcode"},
		"errmsg"=>$dbresref->{"errmsg"}
	} if $dbresref->{"errcode"};
	return {
		"result"=>$dbresref->{"result"}
	};
}

#根据任务号清空目标表
sub truncatetab{
	my ($etlid,$dbname,$dbuser,$dbpwd)=@_;
	return {
		"errcode"=>1,
		"errmsg"=>"参数错误"
	} if @_ != 4;
	my $tartb=&getdbresref($dbname,$dbuser,$dbpwd,
		"select upper(a.tns_name),upper(a.db_user),upper(a.target_tab) from table_list a where a.etl_id='".$etlid."' and a.table_type='1'");
	return {
		"errcode"=>$tartb->{"errcode"},
		"errmsg"=>$tartb->{"errmsg"}
	} if $tartb->{"errcode"};
	my $tdbpwd=&getdbres($dbname,$dbuser,$dbpwd,
		"select a.db_pass from db_user a where upper(a.db_user)='".${$tartb->{"result"}}[0][1]."' and upper(tns_name)='".${$tartb->{"result"}}[0][0]."'");
	return {
		"errcode"=>$tdbpwd->{"errcode"},
		"errmsg"=>$tdbpwd->{"errmsg"}
	} if $tdbpwd->{"errcode"};
	return {
		"errcode"=>1,
		"errmsg"=>${$tartb->{"result"}}[0][0].".".${$tartb->{"result"}}[0][1]."密码不存在"
	} unless defined $tdbpwd->{"result"};
	my $dbh=DBI->connect("DBI:Oracle:".${$tartb->{"result"}}[0][0],${$tartb->{"result"}}[0][1],$tdbpwd->{"result"});
	return {
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr
	} if DBI->err;
	print "truncate table ".${$tartb->{"result"}}[0][2]."\n";
	$dbh->do("truncate table ".${$tartb->{"result"}}[0][2]);
	return {
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr
	} if DBI->err;
	return{"errcode"=>0};
}

#根据任务号对目标本作分析
sub gathertabstat{
	my ($etlid,$dbname,$dbuser,$dbpwd)=@_;
	return {
		"errcode"=>1,
		"errmsg"=>"参数错误"
	} if @_ != 4;
	my $tartb=&getdbresref($dbname,$dbuser,$dbpwd,
		"select upper(a.tns_name),upper(a.db_user),upper(a.target_tab) from table_list a where a.etl_id='".$etlid."' and a.table_type='1'");
	return {
		"errcode"=>$tartb->{"errcode"},
		"errmsg"=>$tartb->{"errmsg"}
	} if $tartb->{"errcode"};
	my $tdbpwd=&getdbres($dbname,$dbuser,$dbpwd,
		"select a.db_pass from db_user a where upper(a.db_user)='".${$tartb->{"result"}}[0][1]."' and upper(tns_name)='".${$tartb->{"result"}}[0][0]."'");
	return {
		"errcode"=>$tdbpwd->{"errcode"},
		"errmsg"=>$tdbpwd->{"errmsg"}
	} if $tdbpwd->{"errcode"};
	return {
		"errcode"=>1,
		"errmsg"=>${$tartb->{"result"}}[0][0].".".${$tartb->{"result"}}[0][1]."密码不存在"
	} unless defined $tdbpwd->{"result"};
	my $dbh=DBI->connect("DBI:Oracle:".${$tartb->{"result"}}[0][0],${$tartb->{"result"}}[0][1],$tdbpwd->{"result"});
	return {
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr
	} if DBI->err;
	print "BEGIN dbms_stats.gather_table_stats(ownname => '".${$tartb->{"result"}}[0][1]."',tabname => '".${$tartb->{"result"}}[0][2]."');END\n";
	$dbh->do("BEGIN dbms_stats.gather_table_stats(ownname => '".${$tartb->{"result"}}[0][1]."',tabname => '".${$tartb->{"result"}}[0][2]."');END;");
	return {
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr
	} if DBI->err;
	return{"errcode"=>0};
}

#unusable index
sub unidx{
	my ($etlid,$dbname,$dbuser,$dbpwd)=@_;
	return {
		"errcode"=>1,
		"errmsg"=>"参数错误"
	} if @_ != 4;
	my $tartb=&getdbresref($dbname,$dbuser,$dbpwd,
		"select upper(a.tns_name),upper(a.db_user),upper(a.target_tab) from table_list a where a.etl_id='".$etlid."' and a.table_type='1'");
	return {
		"errcode"=>$tartb->{"errcode"},
		"errmsg"=>$tartb->{"errmsg"}
	} if $tartb->{"errcode"};
	my $tdbpwd=&getdbres($dbname,$dbuser,$dbpwd,
		"select a.db_pass from db_user a where upper(a.db_user)='".${$tartb->{"result"}}[0][1]."' and upper(tns_name)='".${$tartb->{"result"}}[0][0]."'");
	return {
		"errcode"=>$tdbpwd->{"errcode"},
		"errmsg"=>$tdbpwd->{"errmsg"}
	} if $tdbpwd->{"errcode"};
	return {
		"errcode"=>1,
		"errmsg"=>${$tartb->{"result"}}[0][0].".".${$tartb->{"result"}}[0][1]."密码不存在"
	} unless defined $tdbpwd->{"result"};
	my $idxresref=&getdbresref(${$tartb->{"result"}}[0][0],${$tartb->{"result"}}[0][1],$tdbpwd->{"result"},
		"select index_name from user_indexes a where a.table_owner='".${$tartb->{"result"}}[0][1]."' and a.index_type='NORMAL' and a.table_name='".${$tartb->{"result"}}[0][2]."'");
	return {
		"errcode"=>$idxresref->{"errcode"},
		"errmsg"=>$idxresref->{"errmsg"}
	} if $idxresref->{"errcode"};
	my $dbh=DBI->connect("DBI:Oracle:".${$tartb->{"result"}}[0][0],${$tartb->{"result"}}[0][1],$tdbpwd->{"result"});
	return {
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr
	} if DBI->err;
	foreach my $idx (@{$idxresref->{"result"}}){
		$dbh->do("alter index ".$$idx[0]." unusable");
		print "alter index ".$$idx[0]." unusable\n";
		return {
			"errcode"=>DBI->err,
			"errmsg"=>DBI->errstr
		} if DBI->err;
	}
	$dbh->disconnect;
	return;
}

#rebuild index
sub reidx{
	my ($etlid,$dbname,$dbuser,$dbpwd)=@_;
	return {
		"errcode"=>1,
		"errmsg"=>"reidx参数错误"
	} if @_ != 4;
	my $tartb=&getdbresref($dbname,$dbuser,$dbpwd,
		"select upper(a.tns_name),upper(a.db_user),upper(a.target_tab) from table_list a where a.etl_id='".$etlid."' and a.table_type='1'");
	return {
		"errcode"=>$tartb->{"errcode"},
		"errmsg"=>$tartb->{"errmsg"}."(reidx)"
	} if $tartb->{"errcode"};
	my $tdbpwd=&getdbres($dbname,$dbuser,$dbpwd,
		"select a.db_pass from db_user a where upper(a.db_user)='".${$tartb->{"result"}}[0][1]."' and upper(tns_name)='".${$tartb->{"result"}}[0][0]."'");
	return {
		"errcode"=>$tdbpwd->{"errcode"},
		"errmsg"=>$tdbpwd->{"errmsg"}."(reidx)"
	} if $tdbpwd->{"errcode"};
	return {
		"errcode"=>1,
		"errmsg"=>${$tartb->{"result"}}[0][0].".".${$tartb->{"result"}}[0][1]."密码不存在(reidx)"
	} unless defined $tdbpwd->{"result"};
	my $idxresref=&getdbresref(${$tartb->{"result"}}[0][0],${$tartb->{"result"}}[0][1],$tdbpwd->{"result"},
		"select index_name from user_indexes a where a.table_owner='".${$tartb->{"result"}}[0][1]."' and a.index_type='NORMAL' and a.table_name='".${$tartb->{"result"}}[0][2]."'");
	return{
		"errcode"=>$idxresref->{"errcode"},
		"errmsg"=>$idxresref->{"errmsg"}
	} if $idxresref->{"errcode"};
	my $dbh=DBI->connect("DBI:Oracle:".${$tartb->{"result"}}[0][0],${$tartb->{"result"}}[0][1],$tdbpwd->{"result"});
	return {
		"errcode"=>DBI->err,
		"errmsg"=>DBI->errstr
	} if DBI->err;
	foreach my $idx (@{$idxresref->{"result"}}){
		$dbh->do("alter index ".$$idx[0]." rebuild");
		print "alter index ".$$idx[0]." rebuild\n";
		return {
			"errcode"=>DBI->err,
			"errmsg"=>DBI->errstr
		} if DBI->err;
	}
	$dbh->disconnect;
	return;
}

print "参数错误(y/n)" and exit 1 if @ARGV != 1 or ($ARGV[0] ne "y" and $ARGV[0] ne "n");

my ($conncfgf,$stepgroup,$truncatetablist)=("/ccicall/dc/script/config/conn.cfg","SYNSTEPAU",undef);
open(CFG,$conncfgf) or die $conncfgf."not exists";
our %conncfg=map{chomp;my @row=split/\=/;$row[0]=>$row[1] if $row[0]=~/^[^#]/} <CFG>;
if($ARGV[0] eq "y"){
	my $dbht=DBI->connect("DBI:Oracle:".$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
	$dbht->do("truncate table dc_control_table_loop");
}
my $tasksteps=&getdbresref($conncfg{"dbname"},
		$conncfg{"dbuser"},
		$conncfg{"dbpwd"},
		"select DISTINCT TASK_TYPE
	  from task_list a
	 where a.task_type like '".$stepgroup."\%'
	 and a.task_type >='".$stepgroup."_03'
	 ORDER BY A.TASK_TYPE");
&writedblog(0,"","",$tasksteps->{"errcode"},$tasksteps->{"errmsg"},&currenttime,&currenttime,"") and exit if $tasksteps->{"errcode"};

#目标本表最后一次的加载步骤
my $maxstep=&getdbresref(
	$conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	"select upper(b.target_tab), max(a.task_type)
	from task_list a, table_list b
	where a.etl_id = b.etl_id
	and a.task_type like '".$stepgroup."\%'
	and b.table_type = '1'
	group by upper(b.target_tab)");
&writedblog(0,"","",$maxstep->{"errcode"},$maxstep->{"errmsg"},&currenttime,&currenttime,"") and exit if $maxstep->{"errcode"};
my $tbmaxstep;
foreach my $row(@{$maxstep->{"result"}}){
	$tbmaxstep->{$$row[0]}=$$row[1];
}
#循环步骤
foreach my $steps (@{$tasksteps->{"result"}}){
	print $$steps[0]."\n";
	my $dbresref=&getdbresref($conncfg{"dbname"},
		$conncfg{"dbuser"},
		$conncfg{"dbpwd"},
		"select etl_id from TASK_LIST where task_type='".$$steps[0]."'");
	&writedblog(0,"","",$dbresref->{"errcode"},$dbresref->{"errmsg"},&currenttime,&currenttime,"") and next if $dbresref->{"errcode"};
	my $gathertblist;
	#循环步骤内任务
	foreach my $task (@{$dbresref->{"result"}}){
		my $tartb=&gettartab($$task[0],$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
		&writedblog(0,"","",$tartb->{"errcode"},$tartb->{"errmsg"},&currenttime,&currenttime,"") and next if $tartb->{"errcode"};
		print "error" unless defined $tartb->{"result"};
		$gathertblist->{$tartb->{"result"}}=$$task[0];
		#目标本如果第一次加载，清空目标本，索引实效
		my $loopcount=&getdbres($conncfg{"dbname"},
			$conncfg{"dbuser"},
			$conncfg{"dbpwd"},
			"select loopcount from dc_control_table_loop where tablename='".$tartb->{"result"}."'");
		if($tartb->{"result"}=~/^DM_PA_/){
			#对于结果表，在循环体内不作清空
			unless(defined $loopcount->{"result"}){
				$loopcount->{"result"}=1;
				my $truncateres=&truncatetab($$task[0],$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
				&writedblog(0,"","",$truncateres->{"errcode"},$truncateres->{"errmsg"},&currenttime,&currenttime,"") and next if $truncateres->{"errcode"};
				my $dbh=DBI->connect("DBI:Oracle:".$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
				&writedblog(0,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") and next if DBI->err;
				$dbh->do("insert into dc_control_table_loop(tablename,loopcount)values('".$tartb->{"result"}."',0)");
				&writedblog(0,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") and next if DBI->err;
				$dbh->disconnect;
				my $unidxres=&unidx($$task[0],$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
				&writedblog(0,"","",$unidxres->{"errcode"},$unidxres->{"errmsg"},&currenttime,&currenttime,"") and next if $unidxres->{"errcode"};
			}
		}else{
			unless(exists($truncatetablist->{$tartb->{"result"}})){
				my $truncateres=&truncatetab($$task[0],$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
				&writedblog(0,"","",$truncateres->{"errcode"},$truncateres->{"errmsg"},&currenttime,&currenttime,"") and next if $truncateres->{"errcode"};
				$truncatetablist->{$tartb->{"result"}}=1;
				my $unidxres=&unidx($$task[0],$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
				&writedblog(0,"","",$unidxres->{"errcode"},$unidxres->{"errmsg"},&currenttime,&currenttime,"") and next if $unidxres->{"errcode"};
			}
		}
		system('perl /ccicall/dc/script/dynsql.pl '.$$task[0].' 1');
		print "run task ".$$task[0]."\n";
		my $dbh=DBI->connect("DBI:Oracle:".$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
		&writedblog(0,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") and next if DBI->err;
		$loopcount->{"result"}+=1;
		$dbh->do("update dc_control_table_loop set loopcount=".$loopcount->{"result"}." where tablename='".$tartb->{"result"}."'");
		&writedblog(0,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") and next if DBI->err;
		$dbh->disconnect;
	}
	#对于目标表做完最后一次加载时作表分析及重建索引
	foreach my $tb (keys%{$gathertblist}){
		if($tbmaxstep->{$tb} eq $$steps[0]){
			my $gatherres=&gathertabstat($gathertblist->{$tb},$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
			&writedblog(0,"","",$gatherres->{"errcode"},$gatherres->{"errmsg"},&currenttime,&currenttime,"") and next if $gatherres->{"errcode"};
			my $reidxres=&reidx($gathertblist->{$tb},$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
			&writedblog(0,"","",$reidxres->{"errcode"},$reidxres->{"errmsg"},&currenttime,&currenttime,"") and next if $reidxres->{"errcode"};
		}
	}
}

#代理键生成
my $dbh=DBI->connect("DBI:Oracle:pccictst9","dc_inc","dc_inc");
&writedblog(0,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") if DBI->err;
$dbh->do("begin sp_id_mapping_mkii(); end;");
&writedblog(0,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") if DBI->err;
__END__
