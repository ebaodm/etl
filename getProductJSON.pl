use LWP;
use HTTP::Cookies;
use HTTP::Request;
use HTTP::Response;
use JSON;
use Encode;
use Scalar::Util qw(reftype);
use Digest::MD5;
use Data::Dumper;
use DBI;
$|=1;
#传入json obj，入口
sub analysis{
	my ($pk,$obj,$parent,$ptype) = @_;
	my $tmp;
	#将obj&objattr插入product_obj_tmp表
	my $sql1="insert into product_obj_tmp (productid,isRoot,parent,parenttype,";
	my $sql2=" values ('$productid','";
	my $objtype;
	if($pk == -1){
		$sql2.="1','','','";
	}else{
		$sql2.="0','$parent','$ptype','";
	}
	$pk=$obj->{"\@pk"} if exists($obj->{"\@pk"});
	$objtype=$obj->{"\@type"} if exists($obj->{"\@type"});
	foreach my $k (keys%{$obj}){
		my $type=reftype $obj->{$k};
		$type="VALUE" if ! defined $type;
		if($type eq "VALUE" & $k ne "\@pk" & $k ne "\@type"){	#如果是attr插入product_obj_tmp表
			$sql1.=$k.",";
			$sql2.=$obj->{$k}."','";
		}elsif($type eq "HASH" & $k eq "Fields"){	#如果是Fields由getfield函数处理
			getfield($pk,$obj->{$k});	
		}elsif($type eq "HASH" & $k eq "ChildElements"){ #如果是relation由getchild函数处理
			$ptype=$ptype."/".$obj->{"\@type"};
			getchild($pk,$obj->{$k},$parent,$ptype);
		}elsif ($type eq "HASH" & $k eq "TempData"){
			next;
		}else{
			unknowfield($k);
		}
	}
	$sql1.="pk,type)";
	$sql2.=$pk."','".$objtype."')";
	$dbh->do($sql1.$sql2);
	$dbh->commit;
	print DBI->errstr and return if DBI->err;
}
#处理JSON FIELD节点
sub getfield{
	my ($pk,$obj) = @_;
	$dbh->do("delete from Product_field_attr_tmp where productid='$productid' and pk='$pk'");
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
#				cutcodearray($obj->{$k}->{"CodeTableId"},$obj->{$k}->{"CodeTableName"},$obj->{$k}->{$j});
			}
		}
		$sql2h=~s/\,$//;
		$sql2f=~s/\,$//;
		$sql2h.=")";
		$sql2f.=")";
		$dbh->do($sql2h.$sql2f);
		print DBI->errstr and return if DBI->err;
	}
}
#将code插入Product_Attr_Code_tmp
sub cutcodearray{
	my ($pk,$attr,$obj)=@_;
	foreach my $ele(@{$obj}){
		my $hsql="insert into Product_Attr_Code_tmp(productid,CodeTableId,CodeTableName,";
		my $fsql="values ('".$productid."','".$pk."','".$attr."',";
		foreach my $codeattr(keys%{$ele}){
			if(! defined reftype $ele->{$codeattr}){
				$hsql.=$codeattr.",";
				$ele->{$codeattr}=~s/\'/\'\'/g;
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
		print DBI->errstr and return if DBI->err;
	}
}
#处理JSON childelements节点
sub getchild{
	my ($pk,$obj,$parent,$ptype)=@_;
	foreach my $k (keys%{$obj}){
		my $type=reftype $obj->{$k};
		next if $k eq "PolicyPlanList";
		cutarr($pk,$obj->{$k},$parent."/".$k,$ptype) if $type eq "ARRAY";#子类为obj数组，由cutarr切割obj数组
	}	
}
#将childelements节点数组切割，将子类作为json obj递归
sub cutarr{
	my ($pk,$obj,$parent,$ptype)=@_;
	foreach my $ele (@{$obj}){
		analysis($pk,$ele,$parent,$ptype);
	}
}

sub unknowfield{
	print $_[0]." unknowattr!\n" if $_[0] ne '@type' and $_[0] ne '@pk';
}
sub usage{
	die "参数错误，1、json文件地址";
}

sub main{
	print "产品存在变更，开始解析\n";
	my ($parent,$ptype);
	$dbh->do("delete from product_obj_tmp where productid='$productid'");
	print DBI->errstr and return if DBI->err;
	$dbh->do("delete from Product_Attr_Code_tmp where productid='$productid'");
	print DBI->errstr and return if DBI->err;
	$dbh->do("delete from Product_field_attr_tmp where productid='$productid'");
	print DBI->errstr and return if DBI->err;
	analysis(-1,$_[0],$parent,$ptype);
}
system("date");
my ($server,$username,$password)=('172.25.18.37','ADMIN','eBao1234');
my $cookie_jar=HTTP::Cookies->new(file=>'./protal.cookies',autosave=>1);
my $ua=LWP::UserAgent->new;
my $cookie=$ua->cookie_jar($cookie_jar);
#登录
my $res=$ua->get('http://'.$server.'/cas-server/login?service=http%3A%2F%2F'.$server.'%2Fcas-server%2F%2Foauth2.0%2FcallbackAuthorize');
die 'get err ,url=http://'.$server.'/cas-server/login?service=http%3A%2F%2F'.$server.'%2Fcas-server%2F%2Foauth2.0%2FcallbackAuthorize' if ! $res->is_success;
my $lt=$1 if $res->content()=~/<input type=\"hidden\" name=\"lt\" value=\"([^\"]+)/;
my $execution=$1 if $res->content()=~/<input type=\"hidden\" name=\"execution\" value=\"([^\"]+)/;
$res=$ua->post('http://'.$server.'/cas-server/login?service=http%3A%2F%2F'.$server.'%2Fcas-server%2F%2Foauth2.0%2FcallbackAuthorize',
	[
		username=>$username,
		password=>$password,
		lt=>$lt,
		execution=>$execution,
		_eventId=>'submit',
		submit=>'LOGIN',
	],
	'Content-Type'=> 'application/x-www-form-urlencoded',
	'User-Agent'=>'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36'
);
die "登录失败" if ! defined $res->header("Location");
my $code=$1 if $res->header("Location")=~/ticket=(.+)/;
#获取token
$res=$ua->get('http://'.$server.'/cas-server//oauth2.0/accessToken?client_id=key&client_secret=secret&grant_type=authorization_code&redirect_uri=http%3A%2F%2F'.$server.'%2Fportal&code='.$code);
die 'get err ,url=http://'.$server.'/cas-server//oauth2.0/accessToken?client_id=key&client_secret=secret&grant_type=authorization_code&redirect_uri=http%3A%2F%2F'.$server.'%2Fportal&code='.$code if ! $res->is_success;
my $token=$1 if $res->content=~/access_token=(.+)/;
die "token获取失败" if ! defined $token;
$req=HTTP::Request->new(GET=>'http://'.$server.':8080/product/prd/v1/query/getAllProductList?access_token='.$token);
$res=$ua->request($req);
print encode("gbk",decode("utf8",$res->content())) and die 'get http://'.$server.':8080/product/prd/v1/query/getAllProductList error' if ! $res->is_success;
my $productjson=decode_json($res->content) or die "非法JSON";
foreach my $row (@{$productjson}){
	our $dbh=DBI->connect("DBI:Oracle:ccic_dev","dc_admin",'ccic726',{AutoCommit=>0}) or die "连接数据库失败：".DBI->errstr;
#	next if $row->{"BusinessCode"} !~/^D/ and $row->{"BusinessCode"}!~/^EIC|^EID|^ETA|^ETD|^EGZ|^WUA|^WCA|^WTA|^WHB|^WUS|^QYA01|^QJA|^QZA|^QYL|^GGC|^GGE|^GAA|^JAK|^JAB|^ZFG|^BBB|^ZFC|^ZFX|^ZFY|^ZCG|^ZBS|^ZCJ|^ZBZ|^ZCP|^ZCI|^ZCD|^ZCF|^CAD|^CAA|^OQB|^YAC|^YDS|^YIE|^CCZ|^CBA/;
	our $productid=$row->{"ProductId"};
	$dbh->disconnect and next if ! defined $productid;
	$req=HTTP::Request->new(GET=>'http://'.$server.':8080/dd/public/dictionary/mgmt/v1/generateFullUiResourceSchema?modelName=Policy&objectCode=Policy&contextType=-2&referenceId='.$productid.'&access_token='.$token);
	$res=$ua->request($req);
	print encode("gbk",decode("utf8",$res->content()))."\n" and print 'get err ,url=http://'.$server.':8080/dd/public/dictionary/mgmt/v1/generateFullUiResourceSchema?modelName=Policy&objectCode=Policy&contextType=-2&referenceId='.$productid.'&access_token='.$token."\n" and next if ! $res->is_success;
	my $json=new JSON;
	my $jsobj=$json->decode(decode("utf8",$res->content()));
	undef $res;
	undef $req;
	$productid=$jsobj->{'ElementId'};
	$dbh->disconnect and next if ! defined $productid;
	print "处理产品：$productid-".encode("GBK",$jsobj->{'ObjectName'})."\n";
	$dbh->do("delete from product_product where pk='$row->{\"\@pk\"}'");
	$dbh->commit;
	$sth=$dbh->prepare("insert into product_product(
		pk,type,BusinessCode,BusinessObjectId,BusinessUUID,EffectiveFlag,EndDate,IsAutoGenerateChild,
		ProductElementCode,ProductElementId,ProductElementName,ProductId,ProductMasterId,ProductVersion,
		StartDate)values(?,?,?,?,?,?,to_date(?,'yyyy-mm-dd'),?,?,?,?,?,?,?,to_date(?,'yyyy-mm-dd'))") or (print DBI->errstr and $dbh->disconnect and next);
	$sth->bind_param(1,$row->{"\@pk"});
	$sth->bind_param(2,$row->{"\@type"});
	$sth->bind_param(3,$row->{"BusinessCode"});
	$sth->bind_param(4,$row->{"BusinessObjectId"});
	$sth->bind_param(5,$row->{"BusinessUUID"});
	$sth->bind_param(6,$row->{"EffectiveFlag"});
	$sth->bind_param(7,$row->{"EndDate"});
	$sth->bind_param(8,$row->{"IsAutoGenerateChild"});
	$sth->bind_param(9,$row->{"ProductElementCode"});
	$sth->bind_param(10,$row->{"ProductElementId"});
	$sth->bind_param(11,$row->{"ProductElementName"});
	$sth->bind_param(12,$row->{"ProductId"});
	$sth->bind_param(13,$row->{"ProductMasterId"});
	$sth->bind_param(14,$row->{"ProductVersion"});
	$sth->bind_param(15,$row->{"StartDate"});
	$sth->execute or (print DBI->errstr and $dbh->disconnect and  next);
	$dbh->commit;
	$sth->finish;
	my $md5 = Digest::MD5->new;
	$md5->add(Dumper($jsobj));
	my $md5str=$md5->hexdigest;
	$sth=$dbh->prepare("select md5str from PRODUCT_MD5_HIS where productid='$productid'") or (print DBI->errstr and next);
	$sth->execute or (print $dbh->errstr and next);
	my $dbmd5str=$sth->fetchrow_array();
	$sth->finish;
	undef $md5;
	if(defined $dbmd5str){
		if($md5str eq $dbmd5str){
			print "产品未变化，不再重复解析\n";
			system("date");
			$dbh->disconnect;
			next;
		}else{
			main($jsobj);
			$dbh->do("update PRODUCT_MD5_HIS set md5str='$md5str',productname='".$jsobj->{'ObjectName'}."' where productid='$productid'") or (print DBI->errstr and next);
		}
	}else{
		main($jsobj);
		$dbh->do("insert into PRODUCT_MD5_HIS(productid,md5str,productname) values('$productid','$md5str','".$jsobj->{'ObjectName'}."')") or (print DBI->errstr and next);
	}
	$dbh->commit();
	system("date");
	$dbh->disconnect;
}

system("perl /home/oracle/dc/ccic/script/getCodeTable.pl");
system("date");
__END__
