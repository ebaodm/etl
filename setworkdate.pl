use lib '/infa/script/Public';
use MyTools;
use DBI;
use Data::Dumper;
use threads;
use strict;
$|=1;

#��ʼ���ݿ�����
my $conncfg="/infa/script/config/conn.cfg";
open(CFG,$conncfg) or die $conncfg."not exists";#��־
my %conncfg=map{chomp;my @row=split/\=/;$row[0]=>$row[1] if $row[0]=~/^[^#]/} <CFG>;

#��ȡ���ݿ��û�������Ϣ
my $parausers;
my $dbresref=&getdbresref($conncfg{"dbname"},$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	'select upper(TNS_NAME),UPPER(DB_USER),DB_PASS from DB_USER');
die $dbresref->{"errstr"} if $dbresref->{"err"};#��־
foreach my $row (@{$dbresref->{"result"}}){
	$parausers->{$$row[0]}->{$$row[1]}->{"PASSWORD"}=$$row[2];
}

my ($mirrorlist,$synlist);
#��ȡ�����켣��ϵ
$dbresref=&getdbresref($conncfg{"dbname"},
	$conncfg{"dbuser"},
	$conncfg{"dbpwd"},
	'select upper(t.mirror_tns),upper(t.mirror_user),upper(t.mirror_table),upper(t.mirror_syn),upper(t.trail_tns),upper(t.trail_user),upper(t.trail_table),upper(t.trail_syn),upper(t.MIRROR_SYN_USER),upper(t.TRAIL_SYN_USER) from SYN_RESULT t');
print $dbresref->{"errstr"} if $dbresref->{"err"};#��־
foreach my $row (@{$dbresref->{"result"}}){
	$mirrorlist->{$$row[0]}->{$$row[1]}->{$$row[2]}->{"MIRROR_SYN"}=$$row[3];
	$mirrorlist->{$$row[0]}->{$$row[1]}->{$$row[2]}->{"TRAIL_USER"}=$$row[5];
	$mirrorlist->{$$row[0]}->{$$row[1]}->{$$row[2]}->{"TRAIL_TABLE"}=$$row[6];
	$mirrorlist->{$$row[0]}->{$$row[1]}->{$$row[2]}->{"TRAIL_SYN"}=$$row[7];
	$mirrorlist->{$$row[0]}->{$$row[1]}->{$$row[2]}->{"TRAIL_SYN_USER"}=$$row[9];
	$mirrorlist->{$$row[0]}->{$$row[1]}->{$$row[2]}->{"MIRROR_SYN_USER"}=$$row[8];
	$synlist->{$$row[0]}->{$$row[8]}->{$$row[3]}="";
	$synlist->{$$row[0]}->{$$row[9]}->{$$row[3]}="";
}
#���̻߳�ȡ�������ʱ��
my $maxproc=3;
foreach my $db (keys%{$synlist}){
	foreach my $user(keys%{$synlist->{$db}}){
		foreach my $table(keys%{$synlist->{$db}->{$user}}){
			until(threads->list lt $maxproc){
				foreach(threads->list(threads::all)){
				my @tret=$_->join() if $_->is_joinable();
				$synlist->{$tret[0]}->{$tret[1]}->{$tret[2]}=$tret[3];
				sleep 1;
			}
		}
			my $sql="select to_char(max(updatetime),'yyyymmddhh24miss') from ".$user.".".$table;
			my $tret=threads->create({'context'=>'list'},sub{
				my $sql="select to_char(max(updatetime),'yyyymmddhh24miss') from ".$user.".".$table;
				my $dbres=&getdbres($db,$user,$parausers->{$db}->{$user}->{"PASSWORD"},$sql);
			    return($db,$user,$table,$dbres->{"result"});}
		    )
		}
	}
}
#�ȴ��߳�ȫ�����
until(threads->list eq 0){
		foreach(threads->list(threads::all)){
		my @tret=$_->join() if $_->is_joinable();
		$synlist->{$tret[0]}->{$tret[1]}->{$tret[2]}=$tret[3];
		sleep 1;
	}
}
delete $synlist->{""};

#�����ñ��д����ʱ��
my $dbh=getdbh($conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
foreach my $db (keys%{$synlist}){
	foreach my $user(keys%{$synlist->{$db}}){
		foreach my $table(keys%{$synlist->{$db}->{$user}}){

			print $dbh->{"errmsg"} if $dbh->{"errcode"};#��־
			$dbh->{"dbh"}->do("update syn_result set mirror_date=to_date(".
				$synlist->{$db}->{$user}->{$table}.
				",'YYYYMMDDHH24MISS') WHERE UPPER(MIRROR_SYN_USER)='"
				.$user."' and upper(mirror_syn)='".$table."'");
			#ʧ��д��־
			$dbh->{"dbh"}->do("update syn_result set trail_date=to_date(".
				$synlist->{$db}->{$user}->{$table}.
				",'YYYYMMDDHH24MISS') WHERE UPPER(TRAIL_SYN_USER)='"
				.$user."' and upper(TRAIL_syn)='".$table."'");
			#ʧ��д��־
		}
	}
}

#�������ñ��´�����ʱ���
$dbh->{"dbh"}->do("update syn_result a set a.next_workdate=(select case
	when min(t.mirror_date) > min(t.trail_date) then
	min(t.trail_date)
	else
	min(t.mirror_date)
	end
	from SYN_RESULT t)");#ʧ��д��־
$dbh->{"dbh"}->disconnect();

exit;