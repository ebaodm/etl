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
print "参数错误" and exit 1 if @ARGV ne 1;

my ($server,$username,$password,$tnsname,$dbuser,$dbpwd)=('172.25.18.37','ADMIN','eBao1234','ccic_dev','dc_admin','ccic726');
my $cookie_jar=HTTP::Cookies->new(file=>'./protal.cookies',autosave=>1);
my $ua=LWP::UserAgent->new;
my $cookie=$ua->cookie_jar($cookie_jar);
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
my $productid=$ARGV[0];
$req=HTTP::Request->new(GET=>'http://'.$server.':8080/dd/public/dictionary/mgmt/v1/generateFullUiResourceSchemaWithoutField?modelName=Policy&objectCode=POLICY&contextType=-2&referenceId='.$productid.'&access_token='.$token);
$res=$ua->request($req);
print encode("gbk",decode("utf8",$res->content())) and die 'get http://'.$server.':8080/dd/public/dictionary/mgmt/v1/generateFullUiResourceSchemaWithoutField?modelName=Policy&objectCode=POLICY&contextType=-2&referenceId='.$productid.'&access_token='.$token if ! $res->is_success;
my $json=new JSON;
my $jsobj=$json->decode(decode("utf8",$res->content()));
undef $res;
undef $req;
$productid=$jsobj->{'ElementId'};

exit 1 if ! defined $productid;
print "处理产品：$productid-".encode("GBK",$jsobj->{'ObjectName'})."\n";
my $md5 = Digest::MD5->new;
$md5->add(Dumper($jsobj));
my $md5str=$md5->hexdigest;
our $dbh=DBI->connect("DBI:Oracle:".$tnsname,$dbuser,$dbpwd,{AutoCommit=>0}) or die "连接数据库失败：".DBI->errstr;
my $sth=$dbh->prepare("select md5str from PRODUCT_MD5_HIS where productid='$productid'") or (print DBI->errstr and next);
$sth->execute or (print $dbh->errstr and next);
my $dbmd5str=$sth->fetchrow_array();
$sth->finish;
undef $md5;
if(defined $dbmd5str){
	if($md5str eq $dbmd5str){
		print "产品未变化，不再重复解析\n";
		system("date");
		$dbh->disconnect;
		exit 0;
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

sub main{
	print "产品存在变更，开始解析\n";
	my ($parent,$ptype);
	$dbh->do("delete from product_obj_tmp where productid='$productid'");
	print DBI->errstr and return if DBI->err;
	analysis(-1,$_[0],$parent,$ptype);
}

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
__END__