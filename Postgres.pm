# -*-cperl-*-
#
# Persistence::Object::Postgres - Object Persistence with PostgreSQL.
# Copyright (c) 2000 Ashish Gulhati <hash@netropolis.org>
#
# All rights reserved. This code is free software; you can
# redistribute it and/or modify it under the same terms as Perl
# itself.
#
# $Id: Postgres.pm,v 1.17 2000/09/27 02:06:08 cvs Exp $

package Persistence::Object::Postgres;

use DBI;
use Carp;
use Data::Dumper;
use vars qw( $VERSION );

( $VERSION ) = '$Revision: 1.17 $' =~ /\s+([\d\.]+)/;

sub dbconnect {
  my ($class, $dbobj) = @_;
  my %options = (host     => $dbobj->{Host} || '',
		 port     => $dbobj->{Port} || '5432',
		);
  my $username = $dbobj->{Host} || (''.getpwuid $<);
  my $password = $dbobj->{Host} || '';
  my $options = join (';',"dbname=$dbobj->{Database}",
		      grep { /=.+$/ } map { "$_=$options{$_}" } keys %options);
  return undef unless $dbh = DBI->connect("dbi:Pg:$options", $username, $password);
} 

sub new {
  my ($class, %args) = @_; my $self=undef;
  return undef unless my $dope = $args{__Dope};
  $self = $class->load (__Dope => $dope, __Oid => $args{__Oid} )
    if my $oid = $args{__Oid};
  $self->{__Oid} = $oid if $self; $self = {} unless $self; 
  $self->{__Dope} = $dope; 
  delete $args{__Dope}; delete $args{__Oid};
  foreach (keys %args) { $self->{$_} = $args{$_} }
  bless $self, $class;
}

sub load { 
  my ( $class, %args ) = @_; 
  return undef unless my $oid = $args{__Oid} and my $dope = $args{__Dope}; 
  return undef unless my $table = $dope->{Table}; 
  my @keys = keys %{$dope->{Template}};
  my $selfields = join ',', '__dump', map { $dope->{Template}->{$_} } @keys;
  my $s = $dope->{__DBHandle}->prepare("select $selfields from $table where oid=$oid");
  $s->execute(); return undef unless $s->rows(); my @row = $s->fetchrow_array();
  $object = eval $row[0]; $object->{__Dope} = $dope; $object->{__Oid} = $oid;
  my $i = 0; foreach (@keys) { $object->{$_} = $object->{$_} eq 'ref'?eval $row[++$i]:$row[++$i] }
  return $object; 
}

sub values {
  my ($class, %args) = @_;
  return undef unless my $key = $args{Key} and my $dope = $args{__Dope};
  return undef unless my $table = $dope->{Table} and my $field = $dope->{Template}->{$key};
  my $s = $dope->{__DBHandle}->prepare("select * from $table where oid=0"); $s->execute();
  return undef unless grep { $_ eq $field } @{$s->{NAME}};
  $s = $dope->{__DBHandle}->prepare("select oid,$field from $table"); $s->execute(); 
  return undef unless my $n = $s->rows(); map { $s->fetchrow_array() } (1..$n);
}

sub dumper {
  my $self = shift; 
  $self->{__Dumper} = new Data::Dumper ([$self]); 
  return $self->{__Dumper}; 
}

sub commit {
  my ($self, %args) = @_; return undef unless ref $self;
  return undef unless my $dope = $self->{__Dope}; 
  return undef unless my $table = $dope->{Table};
  my $r; my %tablecols; my $query; my $oid = $self->{__Oid} || 0; 
  my $d = $self->{__Dumper} || $self->dumper (); my @tablecols = ();
  for ( keys %$self ) { delete $self->{ $_ } if /^__(?:Dumper|Dope|Oid)/ }; 

  my %dd = %$self;
  my $dumper = defined &Data::Dumper::Dumpxs?$d->Dumpxs():$d->Dump(); 
  $dumper =~ s/(?<=[^\\])\'/\\\'/sg; my @dump = map { "$_\n" } split ( /\n/, $dumper);
  my $indent = $dump[1]; $indent =~ s/\S.*\n//; $indent = (length $indent)+2;

  $s = $dope->{__DBHandle}->prepare("select * from $table where oid=$oid");
  $s->execute(); my @fields = @{$s->{NAME}};
  unless (grep { $_ eq '__dump' } @fields) {
    my $s = $dope->{__DBHandle}->prepare
      ("alter table $table add column __dump text");
    $s->execute(); return undef unless $s->rows();
  }
    
  foreach $key (keys %{$dope->{Template}}) {
    unless (grep { $_ eq $dope->{Template}->{$key} } @fields) {
      if ($dope->{Createfields}) {
	my $s = $dope->{__DBHandle}->prepare
	  ("alter table $table add column $dope->{Template}->{$key} text");
	$s->execute(); return undef unless $s->rows();
      }
      else {
	next;
      }
    }
    my $i = 0; my $indent2 = (length $key)+$indent+4;
    my $stringified = join '', grep { $i=0 if /^.{$indent}(?!$key)\S/;
                                      $i=1 if /^.{$indent}$key/; $i;
                                    } @dump;
    $stringified =~ s/^(.\s+)\\\'$key\\\'/$1\'$key\'/s; 
    $stringified =~ s/^.{$indent2}//mg; $stringified=~s/,?\n?$//s;
    $stringified =~ s/^(\\\')?/\'/s; $stringified =~ s/(\\\')?$/\'/s; 
    $tablecols{$dope->{Template}->{$key}} = $stringified;
    if (ref $dd{$key}) { $dd{$key} = 'ref' } else { delete $dd{$key} };
  }

  my $dd = bless \%dd, ref $self; $d = new Data::Dumper ([$dd]); 
  $dumper = defined &Data::Dumper::Dumpxs?$d->Dumpxs():$d->Dump(); 
  $dumper =~ s/\'/\\\'/sg; $dumper = "'$dumper'"; 

  $s = $dope->{__DBHandle}->prepare("select * from $table where oid=$oid");
  my $n = $s->execute(); my @fields = @{$s->{NAME}};
  
  if ($n and $oid!=0) {
    $query = "update $table set " . 
             join (',', (map { "$_=$tablecols{$_}" } keys %tablecols),
		   "__dump=$dumper" ) . " where oid=$oid";
  }
  else {
    my @insert = ();
    for (@fields) {
      push (@insert, $dumper), next if $_ eq '__dump';
      push @insert, $tablecols{$_};
    }
    $query = "insert into $table values (" . join (',',@insert) . ')';
  }
  
  $query =~ s/(?<=[,(]),/'',/sg; $query =~ s/,(?=\))/,''/sg;
  $query =~ s/''/NULL/g;
  $s = $dope->{__DBHandle}->prepare($query); print "$query\n";
  $s->execute(); return undef unless $s->rows();
  $self->{__Dope} = $dope; 
  $self->{__Oid} = $oid || $s->{pg_oid_status};
}

sub expire { 
  my ($self, %args) = @_; return undef unless ref $self;
  return undef unless my $oid = $self->{__Oid} and my $dope = $self->{__Dope};
  return undef unless my $table = $dope->{Table};
  my $s = $dope->{__DBHandle}->prepare("select oid from $table where oid=$oid");
  $s->execute(); return undef unless $s->rows();
  $s = $dope->{__DBHandle}->prepare("delete from $table where oid=$oid");
  $s->execute();
} 

sub AUTOLOAD {
  my ($self, $val) = @_; (my $auto = $AUTOLOAD) =~ s/.*:://;
  if ($auto =~ /^(dope|oid)$/) {
    $self->{"__\u$auto"} = $val if defined $val;
    return $self->{"__\u$auto"};
  }
  else {
    croak "Could not AUTOLOAD method $auto.";
  }
}


'True Value';

__END__

=head1 NAME

Persistence::Object::Postgres - Object Persistence with PostgreSQL. 

=head1 SYNOPSIS

  use Persistence::Database::SQL;

  my $db = new Persistence::Database::SQL
    ( Engine => 'Postgres',
      Database => $database_name, 
      Table => $table_name,
      Template => $template_hashref );

  my $object1 = new Persistence::Object::Postgres
    ( __Dope => $db,
      $key => $value );

  my $object2 = new Persistence::Object::Postgres
    ( __Dope => $db, 
      __Oid => $object_id );

  $object1->{$key} = $object2->{$key};

  $object_id = $object1->commit();
  $object2->expire();

=head1 DESCRIPTION

This module provides persistence facilities to its objects. Object
definitions are stored in a PostgreSQL database as stringified perl
data structures, generated with Data::Dumper. Persistence is achieved
with a blessed hash container that holds the object data.

Using a template mapping object properties to PostgreSQL class fields,
it is possible to automatically generate PostgreSQL fields out of the
object data, which allows you to use poweful PostgreSQL indexing and
querying facilities on your database of persistent objects.

This module is intended for use in conjunction with the object
database class Persistence::Database::SQL, which provides persistent
object database handling functionality for multiple DBMS back-ends.
Persistence::Object::Postgres is the module that implements methods
for the PostgreSQL back-end.

=head1 CONSTRUCTOR 

=over 2

=item B<new()>

Creates a new Persistent Object.

  my $object = new Persistence::Object::Postgres 
    ( __Dope => $database );

Takes a hash argument with following possible keys:

B<__Dope> 

The Database of Persistent Entities. This attribute is required and
should have as its value a Persistence::Database::SQL object
corresponding to the database being used.

B<__Oid> 

An optional Object ID. If this attribute is specified, an attempt is
made to load the corresponding persistent object. If no corresponding
object exists, this attribute is silently ignored.

=back 

=head1 OBJECT METHODS

=over 2

=item B<commit()> 

Commits the object to the database.

  $object->commit(); 

=item B<expire()> 

Irrevocably destroys the object. Removes the persistent entry from the
DOPE.

  $object->expire(); 

If you want to keep a backup of the object before destroying it, use
commit() to store it in a different table or database.

  $db->table('expired');
  $object->commit;
  $db->table('active');
  $object->expire(); 

=item B<dumper()> 

Returns the Data::Dumper instance bound to the object.  Should be
called before commit() to change Data::Dumper behavior.

  my $dd = $object->dumper(); 
  $dd->purity(1); 
  $dd->terse(1);    # Smaller dumps. 
  $object->commit(); 

=back

=head1 Inheriting Persistence::Object::Postgres

In most cases you would want to inherit this module to provide
persistence for your own classes. If you use your objects to store
refs to class data, you'd need to bind and detach these refs at load()
and commit(). Otherwise, you'll end up with a separate copy of class
data for every object which will eventually break your code. See
perlobj(1), perlbot(1), and perltoot(1), on why you should use objects
to access class data.

=head1 BUGS

=over 2

=item * 

Error checking needs work. 

=item * 

__Oid is ignored by new() if an object of this ID doesn't already
exist. That's because Postgres generates an oid for us at commit()
time. This is a potential compatibility issue as many other database
engines don't work like postgres in this regard. 

A more generic solution would be to ignore the Postgres oid field
and create a unique identifier of our own at commit(), or use the
user specified __Oid. This will probably be implemented in a future
version, but code written with the assumption that __Oid is ignored
should still work fine. __Oid just won't be ignored, is all.

=head1 SEE ALSO 

Persistence::Database::SQL(3), 
Data::Dumper(3), 
Persistence::Object::Simple(3), 
perlobj(1), perlbot(1), perltoot(1).

=head1 AUTHOR

Persistence::Object::Postgres is Copyright (c) 2000 Ashish Gulhati
<hash@netropolis.org>. All Rights Reserved.

=head1 ACKNOWLEDGEMENTS

Thanks to Barkha for inspiration, laughs and all 'round good times; to
Vipul for Persistence::Object::Simple, the constant use and abuse of
which resulted in the writing of this module; and of-course, to Larry
Wall, Richard Stallman, and Linus Torvalds.

=head1 LICENSE

This code is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

It would be nice if you would mail your patches to me, and I would
love to hear about projects that make use of this module.

=head1 DISCLAIMER

This is free software. If it breaks, you own both parts.

=cut
