use JSON;
use Scalar::Util qw(reftype);
use Encode;
use Digest::MD5;
use Data::Dumper;
use DBI;
=pod
������ƷJSON
��Σ�1��json�ļ���ַ
			2����ƷID
=cut
#����json obj�����
sub analysis{
	my ($pk,$obj,$parent) = @_;
	my $tmp;
	#��obj&objattr����product_obj_tmp��
	my $sql1="insert into product_obj_tmp (productid,isRoot,parent,";
	my $sql2=" values ('$productid','";
	my $objtype;
	if($pk == -1){
		$sql2.="1','','";
	}else{
		$sql2.="0','".$parent."','";
	}
	$pk=$obj->{"\@pk"} if exists($obj->{"\@pk"});
	$objtype=$obj->{"\@type"} if exists($obj->{"\@type"});
	foreach my $k (keys%{$obj}){
		my $type=reftype $obj->{$k};
		$type="VALUE" if ! defined $type;
		if($type eq "VALUE" & $k ne "\@pk" & $k ne "\@type"){	#�����attr����product_obj_tmp��
			$sql1.=$k.",";
			$sql2.=$obj->{$k}."','";
		}elsif($type eq "HASH" & $k eq "Fields"){	#�����Fields��getfield��������
			getfield($pk,$obj->{$k});	
		}elsif($type eq "HASH" & $k eq "ChildElements"){ #�����relation��getchild��������
			getchild($pk,$obj->{$k},$parent);
		}else{
			unknowfield($k);
		}
	}
	$sql1.="pk,type)";
	$sql2.=$pk."','".$objtype."')";
	my $dbh=DBI->connect("DBI:Oracle:".$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
	die DBI->errstr if DBI->err;
	$dbh->do($sql1.$sql2);
	die DBI->errstr if DBI->err;
	$dbh->disconnect;
}
#����JSON FIELD�ڵ�
sub getfield{
	my ($pk,$obj) = @_;
	my $dbh=DBI->connect("DBI:Oracle:".$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
	die DBI->errstr if DBI->err;
	foreach my $k (keys%{$obj}){
		my $sql2h="insert into Product_field_attr_tmp (productid,pk , field_name,";
		my $sql2f=" values ('".$productid."','".$pk."','".$k."',";
		foreach my $j (keys%{$obj->{$k}}){
			my $type=reftype $obj->{$k}->{$j};
			if (! defined $type){
				$sql2h.=$j.",";
				$obj->{$k}->{$j}=~s/\'/\'\'/g;
				$sql2f.="'".$obj->{$k}->{$j}."',";
			}else{
				if(! exists($obj->{$k}->{"CodeTableId"})){
					unknowfield($k)	and next;
				}
				cutcodearray($obj->{$k}->{"CodeTableId"},$obj->{$k}->{"CodeTableName"},$obj->{$k}->{$j});
			}
		}
		$sql2h=~s/\,$//;
		$sql2f=~s/\,$//;
		$sql2h.=")";
		$sql2f.=")";
		$dbh->do($sql2h.$sql2f);
		die DBI->errstr if DBI->err;
	}
	$dbh->disconnect();
}
#��code����Product_Attr_Code_tmp
sub cutcodearray{
	my ($pk,$attr,$obj)=@_;
	my $dbh=DBI->connect("DBI:Oracle:".$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
	die DBI->errstr if DBI->err;
	foreach my $ele(@{$obj}){
		my $hsql="insert into Product_Attr_Code_tmp(productid,CodeTableId,CodeTableName,";
		my $fsql="values ('".$productid."','".$pk."','".$attr."',";
		foreach my $codeattr(keys%{$ele}){
			if(! defined reftype $ele->{$codeattr}){
				$hsql.=$codeattr.",";
				$fsql.="'".$ele->{$codeattr}."',";
			}elsif ($codeattr eq "ConditionFields" and reftype $ele->{$codeattr} eq "ARRAY"){
				next;
			}else{
				unknowfield($codeattr);
			}
		}
		$hsql=~s/\,$//;
		$fsql=~s/\,$//;
		$hsql.=")";
		$fsql.=")";
		$dbh->do($hsql.$fsql);
		die DBI->errstr if DBI->err;
	}
	$dbh->disconnect();
}
#����JSON childelements�ڵ�
sub getchild{
	my ($pk,$obj,$parent)=@_;
	foreach my $k (keys%{$obj}){
		my $type=reftype $obj->{$k};
		cutarr($pk,$obj->{$k},$parent."/".$k) if $type eq "ARRAY";#����Ϊobj���飬��cutarr�и�obj����
	}	
}
#��childelements�ڵ������и��������Ϊjson obj�ݹ�
sub cutarr{
	my ($pk,$obj,$parent)=@_;
	foreach my $ele (@{$obj}){
		analysis($pk,$ele,$parent);
	}
}

sub unknowfield{
	die $_[0]."unknowfield!\n" if $_[0] ne '@type' and $_[0] ne '@pk';
}
sub usage{
	die "��������1��json�ļ���ַ";
}
#--------------------------main---------------------------
usage() if @ARGV ne 1;
open(JSFI,$ARGV[0]) or die "can not open file:".$ARGV[0];
my $dbconf="/infa/script/config/conn.cfg";
open(DBCONF,$dbconf) or die "can not open file:".$dbconf;
our %conncfg=map{chomp;my @row=split/\=/;$row[0]=>$row[1] if $row[0]=~/^[^#]/} <DBCONF>;
close(DBCONF);
my $json=new JSON;
my $js;
#��JSON����תΪscalar
while(<JSFI>){
	$js.=decode("utf8",$_);
}
close(JSFI);
my $jsobj=$json->decode($js);
our $productid=$jsobj->{'ELementId'};
print "�����Ʒ��$productid-".encode("gbk",$jsobj->{'ObjectName'})."\n";
my $md5 = Digest::MD5->new;
$md5->add(Dumper($jsobj));
my $md5str=$md5->hexdigest;
my $dbh=DBI->connect("DBI:Oracle:".$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
my $sth=$dbh->prepare("select md5str from PRODUCT_MD5_HIS where productid='$productid'");
$sth->execute;
my $dbmd5str=$sth->fetchrow_array();
$sth->finish;
if(defined $dbmd5str){
	if($md5str eq $dbmd5str){
		print "��Ʒδ�仯�������ظ�����\n";
		exit 0;
	}else{
		main($jsobj);
		$dbh->do("update PRODUCT_MD5_HIS set md5str='$md5str' where productid='$productid'");
	}
}else{
	main($jsobj);
	$dbh->do("insert into PRODUCT_MD5_HIS(productid,md5str) values('$productid','$md5str')");
}

$dbh->disconnect;
sub main{
	print "��Ʒ���ڱ������ʼ����\n";
	my $parent;
	$dbh=DBI->connect("DBI:Oracle:".$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
	die DBI->errstr if DBI->err;
	$dbh->do("delete from product_obj_tmp where productid='$productid'");
	die DBI->errstr if DBI->err;
	$dbh->do("delete from Product_Attr_Code_tmp where productid='$productid'");
	die DBI->errstr if DBI->err;
	$dbh->do("delete from Product_field_attr_tmp where productid='$productid'");
	die DBI->errstr if DBI->err;
	analysis(-1,$jsobj,$parent);
}
__END__