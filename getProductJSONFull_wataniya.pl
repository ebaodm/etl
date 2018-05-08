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

my @productlist=('73102861','72818872','73104170','72815248','72942449','73408544','72821848','72955917','72817790','72815067','73614133','73092862','72897512','73174491','73614157','72897446','72813152','72824613','72821071','72816468','72859786','72812611','72819901','72812340','72871408','72818884','72877633','72812819','72819515','72829092','73111152','72885966','72821727','72812796','73055981','72811929','72863405','72814119','72823702','72819189','73228844','72875572','73036229');
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
			$obj->{$k}=~s/\'/\'\'/g;
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
			cutcodearray($obj->{$k}->{"CodeTableId"},$obj->{$k}->{"CodeTableName"},$obj->{$k}->{$j});
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
my ($server,$username,$password)=('172.25.16.185','ADMIN','eBao1234');
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
#$req=HTTP::Request->new(GET=>'http://'.$server.':8080/product/prd/v1/query/getAllProductList?access_token='.$token);
#$res=$ua->request($req);
#print $res->content and die 'get http://'.$server.':8080/product/prd/v1/query/getAllProductList error' if ! $res->is_success;
#my $productjson=decode_json($res->content) or die "非法JSON";
our $dbh=DBI->connect("DBI:Oracle:o46g4","wataniya_src_2",'wataniya_src_2') or die "连接数据库失败：".DBI->errstr;
our $productid;
foreach my $productidloop (@productlist){
	#next if $row->{"BusinessCode"} !~/^D/ and $row->{"BusinessCode"}!~/^EIC|^EID|^ETA|^ETD|^EGZ|^WUA|^WCA|^WTA|^WHB|^WUS|^QYA01|^QJA01|^QZA01|^QYL01|^GGC01|^GGE01|^GAA01|^JAK01|^JAB02|^ZFG01|^BBB01|^ZFC01|^ZFX01|^ZFY01|^ZCG01|^ZBS01|^ZCJ01|^ZBZ01|^ZCP01|^ZCI01|^ZCD01|^ZCF01|^CAD01|^CAA02|^OQB02|^YAC04|^YDS03|^YIE02|^CCZ02|^CBA01/;
	$productid=$productidloop;
	$req=HTTP::Request->new(GET=>'http://'.$server.'/restlet/v1/public/dictionary/resource/schema/full?modelName=Policy&objectCode=Policy&contextType=-2&referenceId='.$productid.'&access_token='.$token);
	$res=$ua->request($req);
	print 'get err ,url=http://'.$server.'/restlet/v1/public/dictionary/resource/schema/full?modelName=Policy&objectCode=Policy&contextType=-2&referenceId='.$productid.'&access_token='.$token and next if ! $res->is_success;
	my $json=new JSON;
	my $jsobj=$json->decode(decode("utf8",$res->content()));
	undef $res;
	undef $req;
	#$productid=$jsobj->{'ElementId'};
	print "处理产品：$productid-".encode("GBK",$jsobj->{'ObjectName'})."\n";
	my $md5 = Digest::MD5->new;
	$md5->add(Dumper($jsobj));
	my $md5str=$md5->hexdigest;
	my $sth=$dbh->prepare("select md5str from PRODUCT_MD5_HIS where productid='$productid'") or (print DBI->errstr and next);
	$sth->execute or (print $dbh->errstr and next);
	my $dbmd5str=$sth->fetchrow_array();
	$sth->finish;
	undef $md5;
	if(defined $dbmd5str){
		if($md5str eq $dbmd5str){
			print "产品未变化，不再重复解析\n";
			next;
		}else{
			main($jsobj);
			$dbh->do("update PRODUCT_MD5_HIS set md5str='$md5str',productname='".$jsobj->{'ObjectName'}."' where productid='$productid'") or (print DBI->errstr and next);
		}
	}else{
		main($jsobj);
		$dbh->do("insert into PRODUCT_MD5_HIS(productid,md5str,productname) values('$productid','$md5str','".$jsobj->{'ObjectName'}."')") or (print DBI->errstr and next);
	}
}
$dbh->disconnect;
#system("perl /home/oracle/dc/ccic/script/getCodeTable_wataniya.pl");
system("date");
__END__