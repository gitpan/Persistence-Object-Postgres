# -*-cperl-*-
#
# Persistence::Object::Postgres - Object Persistence with PostgreSQL.
# Copyright (c) 2000 Ashish Gulhati <hash@netropolis.org>
#
# All rights reserved. This code is free software; you can
# redistribute it and/or modify it under the same terms as Perl
# itself.
#
# $Id: Postgres.pm,v 1.5 2000/07/28 09:50:48 cvs Exp $

package Persistence::Object::Postgres;

use Pg;
use Carp;
use Data::Dumper;
use vars qw( $VERSION );

( $VERSION ) = '$Revision: 1.5 $' =~ /\s+([\d\.]+)/;

sub dbconnect {
  my ($class, $dbobj) = @_;
  my %options = (host     => $dbobj->{Host} || '',
		 port     => $dbobj->{Port} || '5432',
		 user     => $dbobj->{Host} || (''.getpwuid $<),
		 password => $dbobj->{Host} || ''
		);
  my $options = join (' ',"dbname=$dbobj->{Database}",
		      grep { /=.+$/ } map { "$_=$options{$_}" } keys %options);
  my $dbconn = Pg::connectdb ($options);
  my $status = $dbconn->status;
  return undef unless $status eq PGRES_CONNECTION_OK;
  return $dbconn;
} 

sub new {
  my ($class, %args) = @_; my $self=undef;
  return undef unless my $dope = $args{__Dope};
  $self = $class->load (__Dope => $dope, __Oid => $oid )
    if my $oid = $args{__Oid};
  $self->{__Oid} = $oid if $self; $self = {} unless $self; 
  $self->{__Dope} = $dope;
  bless $self, $class;
}

sub load { 
  my ( $class, %args ) = @_; 
  return undef unless my $oid = $args{__Oid} and my $dope = $args{__Dope}; 
  return undef unless my $table = $dope->{Table};
  my $r = $dope->{__DBHandle}->exec("select __dump from $table where oid=$oid");
  return undef unless $r->ntuples(); my ($object) = $r->fetchrow();
  $object = eval $object; $object->{__Dope} = $dope; $object->{__Oid} = $oid;
  return $object; 
}

sub values {
  my ($class, %args) = @_;
  return undef unless my $key = $args{Key} and my $dope = $args{__Dope};
  return undef unless my $table = $dope->{Table} and my $field = $dope->{Template}->{$key};
  my $r = $dope->{__DBHandle}->exec("select * from $table where oid=0");
  return undef unless grep { $field eq $_ } map { $r->fname($_) } (0..$r->nfields()-1);
  $r = $dope->{__DBHandle}->exec("select oid,$field from $table");
  map { $r->fetchrow() } (1..$r->ntuples());
}

sub dope {
  my ($self, $dope) = @_;
  ${ $self->{ __Dope } } = $dope if $dope;
  ${ $self->{ __Dope } };
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
  my $d = $self->{__Dumper} || $self->dumper ();
  for ( keys %$self ) { delete $self->{ $_ } if /^__(?:Dumper|Dope|Oid)/ }; 

  my $dumper = defined &Data::Dumper::Dumpxs?$d->Dumpxs():$d->Dump(); 
  $dumper =~ s/\'/\\\'/sg; my @dump = map { "$_\n" } split ( /\n/, $dumper);
  $dumper = "'$dumper'"; my $indent = $dump[1]; 
  $indent =~ s/\S.*\n//; $indent = (length $indent)+2;

  foreach $field (keys %{$dope->{Template}}) {
    my $i = 0; my $indent2 = (length $field)+$indent+4;
    my $stringified = join '', grep { $i=0 if /^.{$indent}(?!$field)\S/;
                                      $i=1 if /^.{$indent}$field/; $i;
                                    } @dump;
    $stringified =~ s/^(.\s+)\\\'$field\\\'/$1\'$field\'/s; 
    $stringified =~ s/^.{$indent2}//mg; $stringified=~s/,?\n?$//s;
    $stringified =~ s/^(\\\')?/\'/s; $stringified =~ s/(\\\')?$/\'/s; 
    $tablecols{$dope->{Template}->{$field}} .= $stringified;
  }

  $r = $dope->{__DBHandle}->exec("select * from $table where oid=$oid");
  if ($r->ntuples and $oid!=0) {
    $query = "update $table set " . 
             join (',', (map { "$_=$tablecols{$_}" } keys %tablecols),
		   "__dump=$dumper" ) . " where oid=$oid";
  }
  else {
    my @insert = ();
    for (0..$r->nfields()-1) {
      my $field = $r->fname($_);
      push (@insert, $dumper), next if $field eq '__dump';
      push @insert, $tablecols{$field};
    }
    $query = "insert into $table values (" . join (',',@insert) . ')';
  }

  $r = $dope->{__DBHandle}->exec($query);
  $self->{__Dope} = $dope; 
  $self->{__Oid} = $oid || $r->oidStatus;
}

sub expire { 
  my ($self, %args) = @_; return undef unless ref $self;
  return undef unless my $oid = $self->{__Oid} and my $dope = $self->{__Dope};
  return undef unless my $table = $dope->{Table};
  my $r = $dope->{__DBHandle}->exec("select oid from $table where oid=$oid");
  return undef unless $r->ntuples();
  $dope->{__DBHandle}->exec("delete from $table where oid=$oid");
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
      Oid => $object_id );

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
