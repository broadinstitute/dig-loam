package config;

# Manages parsing of and access to config files
# Also manages transactional 
#
#

use Carp();

use FindBin;
use lib "$FindBin::Bin/..";

use warnings;
use strict;
use vars;

use bin::trap_sig;
require bin::util;

use Getopt::Long;
Getopt::Long::Configure("pass_through", "no_auto_abbrev");

use File::Temp qw/tempfile/;

use Devel::StackTrace;

use Scalar::Util qw(looks_like_number);

sub get_dir_key($@);
sub get_all_files();
sub search_for_file($);
sub expand_value($@);
sub set_initial_value($$);

#separate properties with colons
my @props = ();
my $prop_arg = "prop";
my $prop_arg_delim = "=";

GetOptions("$prop_arg=s" => \@props);

my %passed_props = ();
foreach my $prop (@props)
{
		(my $key, my $value, my $extra) = split(/$prop_arg_delim/, $prop);
		if (defined $value && !$extra)
		{
				if (exists $passed_props{$key})
				{
						die "Multiple values not allowed for $key\n";
				}
				$passed_props{$key} = $value;
		}
		else
		{
				die "Couldn't parse $prop\n";
		}
}
my %unused_passed_props = %passed_props;

our %map = ();
our %expanded_map = ();
our %initial_map = ();

#To add a keyword, define a constant, and add to prefix or postfix data arrays. 
#Remember to update config.php as well
my $include_prefix = "!";
my $multiple_arg_delim = ",";
my $vector_arg_delim = ":";

my $title_keyword = "title";
my $include_keyword = "include";
my $postfix_keyword = "postfix";
my $expand_keyword = "expand";
my $input_keyword = "input";
my $output_keyword = "output";
my $prop_keyword = "prop";
my $key_keyword = "key";
my $raw_keyword = "raw";

my $if_prop_mod_keyword = "if_prop";
my $unless_prop_mod_keyword = "unless_prop";
my $or_if_prop_mod_keyword = "or_if_prop";
my $def_keyword = "defined";
my $eq_keyword = "eq";
my $ne_keyword = "ne";
my $lt_keyword = "lt";
my $gt_keyword = "gt";
my $le_keyword = "le";
my $ge_keyword = "ge";
my $cmplt_keyword = "cmp_lt";
my $cmpgt_keyword = "cmp_gt";
my $cmple_keyword = "cmp_le";
my $cmpge_keyword = "cmp_ge";
my %valid_prop_ops = ($def_keyword=>1,
											$eq_keyword=>2,
											$ne_keyword=>2,
											$lt_keyword=>2,
											$gt_keyword=>2,
											$le_keyword=>2,
											$ge_keyword=>2,
											$cmplt_keyword=>2,
											$cmpgt_keyword=>2,
											$cmple_keyword=>2,
											$cmpge_keyword=>2);

my $uc_mod_keyword = "uc";
my $all_instances_mod_keyword = "all_instances";
my $optional_mod_keyword = "optional";
my $is_list_mod_keyword = "is_list";
my $sep_mod_keyword = "sep";
my $deref_mod_keyword = "deref";
my $missing_mod_keyword = "missing";
my $missing_prop_mod_keyword = "missing_prop";
my $missing_key_mod_keyword = "missing_key";
my $prop_is_key_mod_keyword = "prop_is_key";
my $flatten_mod_keyword = "flatten";
my $allow_empty_mod_keyword = "allow_empty";
my $limit_mod_keyword = "limit";
my $max_mod_keyword = "max";
my $sort_prop_mod_keyword = "sort_prop";
my $instance_level_mod_keyword = "instance_level";

my %cmd_mod_keywords = ($if_prop_mod_keyword=>1,
												$unless_prop_mod_keyword=>1,
												$or_if_prop_mod_keyword=>1,
												$optional_mod_keyword=>1,
												$is_list_mod_keyword=>1,
												$all_instances_mod_keyword=>1,
												$uc_mod_keyword=>1,
												$sep_mod_keyword=>1,
												$deref_mod_keyword=>1,
												$missing_mod_keyword=>1,
												$missing_prop_mod_keyword=>1,
												$missing_key_mod_keyword=>1,
												$prop_is_key_mod_keyword=>1,
												$flatten_mod_keyword=>1,
												$allow_empty_mod_keyword=>1,
												$limit_mod_keyword=>1,
												$max_mod_keyword=>1,
												$sort_prop_mod_keyword=>1,
												$instance_level_mod_keyword=>1,
		);

my $list_keyword = "list";
my $scalar_keyword = "scalar";

#prefix keyword

my $private_keyword = "private";
my $path_keyword = "path";
my $bam_keyword = "bam";
my $selector_keyword = "selector";
my $mkdir_keyword = "mkdir";
my $sortable_keyword = "sortable";
my $table_keyword = "table";
my $missing_okay_keyword = "missing_okay";
my $optional_input_keyword = "optional_input";
my $optional_output_keyword = "optional_output";
my $nohead_keyword = "nohead";
my $onecol_keyword = "onecol";
my $img_links_keyword = "img_links";
my $doubcom_keyword = "doubcom";
my $major_keyword = "major";
my $minor_keyword = "minor";
my $sort_local_keyword = "sort_local";
my $constant_keyword = "constant";
my $meta_keyword = "meta";
my $include_failed_keyword = "include_failed";
my $deleteable_keyword = "deleteable";
my $class_keyword = "class";
my $cmd_class_keyword = "cmd_class";
my $hidden_output_class_keyword = "hidden_output_class";
my $cmd_keyword = "cmd";
my $meta_table_keyword = "meta_table";
my $no_custom_keyword = "no_custom";
my $local_keyword = "local";
my $short_keyword = "short";
my $file_keyword = "file";
my $cat_keyword = "cat";
my $convert_paths_keyword = "convert_paths";
my $restart_long_keyword = "restart_long";
my $restart_mem_keyword = "restart_mem";
my $ignore_md5_keyword = "ignore_md5";

our $and_conjunction = "and";
our $or_conjunction = "or";

#postfix keyword

my $depends_keyword = "depends";
my $meta_level_keyword = "meta_level";
my $dir_keyword = "dir";
my $xml_keyword = "xml";
my $bai_keyword = "bai";
my $summary_keyword = "sum";
my $summarize_keyword = "summarize";
my $parent_keyword = "parent";
my $consistent_keyword = "consistent";
my $consistent_prop_keyword = "consistent_prop";
my $child_order_keyword = "child_order";
my $link_keyword = "link";
my $disp_keyword = "disp";
my $chmod_keyword = "chmod";
my $key_col_keyword = "key_col";
my $tiebreak_col_keyword = "tiebreak_col";
my $env_mod_keyword = "env_mod";
my $rusage_mod_keyword = "rusage_mod";
my $update_ext_keyword = "update_ext";
my $bsub_batch_keyword = "bsub_batch";
my $run_if_keyword = "run_if";
my $skip_if_keyword = "skip_if";
my $with_keyword = "with";
my $run_with_keyword = "run_with";
my $umask_mod_keyword = "umask_mod";
my $skip_re_keyword = "skip_re";
my $class_level_keyword = "class_level";
my $add_header_keyword = "add_header";
my $goto_url_keyword = "goto_url";
my $comment_keyword = "comment";
my $timeout_keyword = "timeout";
my $default_keyword = "default";

my %file_keywords = 
(
		$include_keyword=>1
);

my %prefix_data = 
		($private_keyword=>{},
		 $path_keyword=>{},
		 $bam_keyword=>{},
		 $selector_keyword=>{},
		 $table_keyword=>{},
		 $missing_okay_keyword=>{},
		 $optional_input_keyword=>{},
		 $optional_output_keyword=>{},
		 $nohead_keyword=>{},
		 $onecol_keyword=>{},
		 $img_links_keyword=>{},
		 $doubcom_keyword=>{},
		 $major_keyword=>{},
		 $minor_keyword=>{},
		 $sort_local_keyword=>{},
		 $constant_keyword=>{},
		 $meta_keyword=>{},
		 $mkdir_keyword=>{},
		 $sortable_keyword=>{},
		 $include_failed_keyword=>{},
		 $deleteable_keyword=>{},
		 $class_keyword=>{},
		 $cmd_class_keyword=>{},
		 $hidden_output_class_keyword=>{},
		 $cmd_keyword=>{},
		 $prop_keyword=>{},
		 $meta_table_keyword=>{},
		 $no_custom_keyword=>{},
		 $local_keyword=>{},
		 $short_keyword=>{},
		 $file_keyword=>{},
		 $cat_keyword=>{},
		 $convert_paths_keyword=>{},
		 $restart_mem_keyword=>{},
		 $restart_long_keyword=>{},
		 $ignore_md5_keyword=>{},
		);

my %postfix_data = 
		($dir_keyword=>{},
		 $depends_keyword=>{},
		 $meta_level_keyword=>{},
		 $bai_keyword=>{},
		 $xml_keyword=>{},
		 $summary_keyword=>{},
		 $summarize_keyword=>{},
		 $link_keyword=>{},
		 $parent_keyword=>{},
		 $consistent_keyword=>{},
		 $consistent_prop_keyword=>{},
		 $child_order_keyword=>{},
		 $disp_keyword=>{},
		 $chmod_keyword=>{},
		 $key_col_keyword=>{},
		 $tiebreak_col_keyword=>{},
		 $env_mod_keyword=>{},
		 $rusage_mod_keyword=>{},
		 $update_ext_keyword=>{},
		 $bsub_batch_keyword=>{},
		 $run_if_keyword=>{},
		 $skip_if_keyword=>{},
		 $with_keyword=>{},
		 $run_with_keyword=>{},
		 $umask_mod_keyword=>{},
		 $skip_re_keyword=>{},
		 $class_level_keyword=>{},
		 $goto_url_keyword=>{},
		 $comment_keyword=>{},
		 $timeout_keyword=>{},
		 $default_keyword=>{},
		 $add_header_keyword=>{},
		);

my %postfix_list_keywords = ($depends_keyword=>1,
														 $meta_level_keyword=>1,
														 $summarize_keyword=>1,
														 $link_keyword=>1,
														 $consistent_keyword=>1,
														 $with_keyword=>1,
														 $run_with_keyword=>1,
														 $key_col_keyword=>1,
														 $env_mod_keyword=>1,
														 $add_header_keyword=>1,
		);

sub init($)
{
		my $config_file = shift;

		my @lines = ();

		open IN, $config_file or die "Can't find config file $config_file\n";
		push @lines, $_ while (<IN>);
		close IN;

		my %to_delete = ();

		my $prepend = "";
		while (@lines)
		{
				$_ = shift @lines;
				chomp;
				next if /^\#/;

				#only strip comment if not escaped
				s/([^\\])#.+/$1/;
				s/\\#/#/g;

				s/^\s+//;
				s/\s+$//;

				next if /^$/;

				#new
				if ($_ =~ /\\$/)
				{
						$prepend .= substr($_, 0, length($_) - 1);
						next;
				}
				$_ = "$prepend$_";
				$prepend = "";
				#end new

				if (/^$include_prefix(.)$expand_keyword/)
				{
						my $end_delim = quotemeta($1);
						if (/^$include_prefix$end_delim$expand_keyword([^$end_delim]+)$end_delim\s*(.+)/)
						{
								my $expand_info = $1;
								my $content = $2;

								if (!$expand_info)
								{
										die $_;
								}

								my $expand_delim = substr($expand_info, 0, 1);
								$expand_info = substr($expand_info, 1);
								my $expand_sep = undef;
								if (length($expand_info) >= 2 && substr($expand_info, 1, 1) eq $expand_delim)
								{
										$expand_sep = substr($expand_info, 0, 1);
										$expand_info = substr($expand_info, 2);
								}
								my @expand_info_array = split($expand_delim, $expand_info, -1);
								if (scalar(@expand_info_array) < 2)
								{
										die "Must have at least two values separated by $expand_delim in $expand_info";
								}
								my @to_replace = ($expand_info_array[0]);
								if (defined $expand_sep)
								{
										@to_replace = split($expand_sep, $expand_info_array[0], -1);
								}
								@to_replace = map {quotemeta($_)} @to_replace;
								for (my $i = $#expand_info_array; $i >= 1; $i--)
								{
										my @replacement = ($expand_info_array[$i]);
										if (defined $expand_sep && $expand_info_array[$i] ne "")
										{
												@replacement = split($expand_sep, $expand_info_array[$i], -1);
										}
										if (scalar(@replacement) != scalar(@to_replace))
										{
												die "Error with $_\nExpansion $expand_info_array[$i] does not have the same number of tokens as template $expand_info_array[0]";
										}
										my $data2 = $content;
										for (my $j = 0; $j < scalar(@to_replace); $j++)
										{
												$data2 =~ s/$to_replace[$j]/$replacement[$j]/g;
										}
										unshift @lines, $data2;
								}
								next;
						}
				}

				if (/^$include_prefix$postfix_keyword\s+(.+)$/)
				{
						$postfix_data{$1} = {};
						next;
				}

				if (/^$include_prefix$include_keyword\s+(.+)$/)
				{
						my $file_to_include = $1;
						$file_to_include = util::search_for_file(expand_value($file_to_include), @INC);
						open IN2, $file_to_include or die "Can't read $file_to_include\n";
						my @file_lines = <IN2>;
						close IN2;
						foreach (reverse @file_lines)
						{
								unshift @lines, $_;
						}
						next;
				}
				elsif (/^$include_prefix$title_keyword\s+(.+)$/)
				{
						next;
				}

				my %cur_prefixes = ();
				my $prefix_re = join ("|", keys %prefix_data);
				while (/^($prefix_re)\s+/)
				{
						my $modifier = $1;
						if (exists $prefix_data{$modifier})
						{
								$cur_prefixes{$modifier} = 1;
						}
						else
						{
								die "There is a bug: somehow matched modifier $modifier\n";
						}
						s/$modifier\s+//;
				}

				my %cur_postfixes = ();
				my $postfix_re = join ("|", keys %postfix_data);
				my $arg_re = '((\"[^\"]*\"|[^\"\s])+)';
				while (/\s+($postfix_re)\s+$arg_re$/)
				{
						my $modifier = $1;
						my $arg = $2;
						my $value = $arg;

						#strip it out of the current parsed text

						$arg =~ s/\$/\\\$/;
						s/\s+$modifier\s+$arg_re$//;

						#now store it
						$value =~ s/\"//;
						$value =~ s/\"//;

						if (exists $postfix_data{$modifier})
						{
								$cur_postfixes{$modifier} = $value;
						}
						else
						{
								die "There is a bug: somehow matched modifier $modifier\n";
						}

				}

				next unless length;
				(my $key, my $value, my @rest) = split(/=/);
				$key =~ s/\s*$// if $key;
				$value =~ s/^\s*// if $value;

				$value = "" unless defined $value;
				$value = join "=", ($value, @rest);

				#possible overide
				if (exists $passed_props{$key})
				{
						if (exists $unused_passed_props{$key})
						{
								delete $unused_passed_props{$key}
						}

						$value = $passed_props{$key};
				}
				elsif (exists $initial_map{$key})
				{
						$value = $initial_map{$key};
				}

				$map{$key} = $value;

				foreach my $cur_prefix (keys %cur_prefixes)
				{
						$prefix_data{$cur_prefix}{$key} = 1;
				}

				foreach my $cur_postfix (keys %cur_postfixes)
				{
						my $arg = $cur_postfixes{$cur_postfix};
						if (exists $postfix_list_keywords{$cur_postfix})
						{
								my @args = split($multiple_arg_delim, $arg);
								foreach (@args)
								{
										push @{$postfix_data{$cur_postfix}{$key}}, $_;
								}
						}
						else
						{
								$postfix_data{$cur_postfix}{$key} = $arg;
						}
				}
		}

		if (scalar keys %unused_passed_props)
		{
				my $msg = "Passed --$prop_arg unused: " . join (",", keys %unused_passed_props);
				die $msg;
		}

		#my %new_map = ();
		#foreach my $key (keys %map)
		#{
				#check to see if value has any $ signs in it
				#$new_map{$key} = expand_value($map{$key}, $key);
		#}
		#have to do this to avoid expanding keys with \$ in them; the \ will be unescaped during expand
		#foreach my $key (keys %new_map)
		#{
		#		$map{$key} = $new_map{$key};
		#}

		foreach my $cur_postfix (keys %postfix_data)
		{
			foreach my $key (keys %{$postfix_data{$cur_postfix}})
			{
					my $new_value = undef;
					if (exists $postfix_list_keywords{$cur_postfix})
					{
							my @new_values = map {expand_value($_)} @{$postfix_data{$cur_postfix}{$key}};
							$new_value = \@new_values;
					}
					else
					{
							$new_value = expand_value($postfix_data{$cur_postfix}{$key});
					}
					$postfix_data{$cur_postfix}{$key} = $new_value
			}
		}

		#validation

		my %file_paths = ();
		foreach my $file_key (get_all_files())
		{
				my $cur_file = $map{$file_key};
				my $cur_dir_key = get_dir_key($file_key);
				my $cur_dir = $map{$cur_dir_key};
				if (!$cur_dir)
				{
						die "Error: dir $cur_dir_key for $cur_file has not been defined\n";
				}
				my $cur_file_path = util::cat_dir_file($cur_dir, $cur_file);
				if ($file_paths{$cur_file_path})
				{
						die "Error in config file: $file_key and $file_paths{$cur_file_path} have the same value; all files must have different values\n";
				}

				$file_paths{$cur_file_path} = $file_key;
		}
}

#This MUST be called before init
sub set_initial_value($$)
{
		my $key = shift;
		my $value = shift;
		$initial_map{$key} = $value;
		if (!exists $map{$key})
		{
				$map{$key} = $value;
		}
}

sub expand_value($@)
{
		my $value = shift;

		my $called_on_key = shift;
		my $new_value = $value;
		my $last_value = undef;
		while ($new_value =~ /(^|[^\\])([\$\*][a-zA-Z0-9\_]+|[\$\*]\{[a-zA-Z0-9\_]+\})(\([^\(\)]*\))?/)
		{
				if (defined $last_value && $last_value eq $new_value)
				{
						die "Error expanding $value: got into infinite loop";
				}
				$last_value = $new_value;
				my $captured = $2;
				my $args = $3;
				my $new_key = $captured;
				if ($args)
				{
						$captured .= $args;
				}
				my $expand_path = 0;
				if ($captured =~ /^\*/)
				{
						$expand_path = 1;
				}

				#in case they have backslashes
				#$captured =~ s/\\/\\\\/g;

				#$captured =~ s/\*/\\\*/g;
				#$captured =~ s/\$/\\\$/g;
				#$captured =~ s/\(/\\\(/g;
				#$captured =~ s/\)/\\\)/g;
				#$captured =~ s/\{([0-9])\}/\\\{$1\\\}/g;
				#$captured =~ s/\,/\\\,/g;
				#$captured =~ s/\"/\\\"/g;
				#$captured =~ s/\|/\\\|/g;
				$captured = quotemeta($captured);

				$new_key =~ s/\$//;
				$new_key =~ s/\*//;
				$new_key =~ s/\{//g;
				$new_key =~ s/\}//g;
				my @args = ();

				if (defined $args && $args ne "")
				{
						$args =~ s/^\(//;
						$args =~ s/\)$//;
						do
						{
								my $cur_arg = "";
								my $delim = "";
								my $rest = "";
								
								if ($args =~ /^\"([^\"]*)\"([^,]*)(,?)(.*)$/)
								{
										$cur_arg = "$1$2";
										$delim = $3;
										$rest = $4;
								}
								else
								{
										if ($args =~ /^(\,)(.*)/ || $args =~ /^(.*?[^\\],)(.*)/ )
										{
												$cur_arg = "$1";
												$rest = $2;
												if ($cur_arg =~ /,$/)
												{
														$delim = substr($cur_arg, length($cur_arg) - 1);
														$cur_arg = substr($cur_arg, 0, length($cur_arg) - 1);
												}
										}
										else
										{
												#last thing to check is if it had no unescaped commas

												my @commas = $args =~ /.,/g;

												foreach my $match (@commas)
												{
														if (substr($match, 0, 1) ne '\\')
														{
																die "Bug: $args in $value did not match a regular expression in the code";
														}
												}
												$cur_arg = $args;
												$rest = "";

										}
										$cur_arg =~ s/\\,/,/g;
										$cur_arg =~ s/\\"/"/g;
								}
								push @args, $cur_arg;
								$args = $rest;

								#boundary case
								if ($delim && (!defined $rest || $rest eq ""))
								{
										push @args, "";
								}

						} while (defined $args && $args ne "");
				}
				if (exists $map{$new_key})
				{
						my $map_value = undef;
						if ($expand_path)
						{
								$map_value = get_path($new_key, 1);
						}
						else
						{
								$map_value = $map{$new_key};
								if (@args)
								{
										my %args = ();
										foreach (my $i = 0; $i <= $#args; $i++)
										{
												$args{$i + 1} = $args[$i];
										}
										$map_value = get_unexpanded_value($new_key, %args);
								}
						}
						$new_value =~ s/$captured/$map_value/;
				}
				else
				{
						my $add = defined $called_on_key ? " expanding $called_on_key" : "";
						die "Error$add in config_file: key $new_key was not defined";
				}
		}
		$new_value =~ s/\\\$/\$/g;
		$new_value =~ s/\\\*/\*/g;

		return $new_value;
}

sub get_expanded_value($)
{
		my $key = shift;
		unless (exists $expanded_map{$key})
		{
				$expanded_map{$key} = expand_value($map{$key}, $key);
		}
		return $expanded_map{$key}
}

sub get_bam_index($)
{
		my $bam_path = shift;
		$bam_path =~ s/\.bam$/\.bai/;
		return $bam_path;
}

sub get_all_keys()
{
		return keys %map;
}

sub get_passed_prop_arg_string()
{
		my $arg_string = "";
		foreach my $prop (keys %passed_props)
		{
				my $prop_value = $passed_props{$prop};
				if ($prop_value =~ /\s/)
				{
						$prop_value = "\"$prop_value\"";
				}
				$arg_string .= " --$prop_arg $prop$prop_arg_delim$prop_value";
		}

		return $arg_string;
}

my %subs_cache = ();
sub get_value($@)
{
		my $key = shift;
		return get_value_int($key, 1, @_);
}

sub get_unexpanded_value($@)
{
		my $key = shift;
		return get_value_int($key, 0, @_);
}

sub get_value_int($@)
{
		my $key = shift;
		my $use_expanded = shift;
		my $cache_key = join(":", @_);

		unless (exists $subs_cache{$key}{$cache_key})
		{
				unless (is_key($key))
				{
						die "Error: Can't get value for $key; not a key\n"; 
				}
				my $value = undef;
				if ($use_expanded)
				{
						$value = get_expanded_value($key);
				}
				else
				{
						$value = $map{$key};
				}
				#pass one extra param to force no arg expansion
				eval
				{
						if (scalar @_ != 1)
						{
								$value = util::substitute_args($value, @_);
						}
				};
				if ($@)
				{
						if ($@ eq main::trap_signal())
						{
								die $@;
						}
						die "Error getting value for $key: $@";
				}
				if (is_path($key))
				{
						$value =~ s/\s/_/g;
				}
				$subs_cache{$key}{$cache_key} = $value;
		}
		return $subs_cache{$key}{$cache_key};

}

sub get_dir($@)
{
		my $key = shift;
		my $dir_key = get_dir_key($key);

		return get_value($dir_key, @_);
}


#keyword parsing
#prefixes

sub is_key($)
{
		my $key = shift;
		return exists $map{$key};
}

sub is_prefix($$)
{
		my $key = shift;
		my $keyword = shift;

		return exists $prefix_data{$keyword}{$key};
}
sub is_bam($)
{
		my $key = shift;

		return is_prefix($key, $bam_keyword);
}

sub is_mkdir($)
{
		my $key = shift;

		return is_prefix($key, $mkdir_keyword);
}

sub is_sortable($)
{
		my $key = shift;

		return is_prefix($key, $sortable_keyword);
}

sub get_all_mkdir()
{
		my $class = shift;
		my @keys = keys %{$prefix_data{$mkdir_keyword}};
		my @to_mk = ();
		foreach my $key (@keys)
		{
				if (is_mkdir($key))
				{
						if ((defined $class && has_class_level($key) && get_class_level($key) eq $class) ||
								(!defined $class && !has_class_level($key)))
						{
								push @to_mk, $key;
						}
				}
		}
		return @to_mk;
}

sub is_deleteable($)
{
		my $key = shift;

		return is_prefix($key, $deleteable_keyword);
}

sub get_all_deleteable()
{
		my @keys = keys %{$prefix_data{$deleteable_keyword}};
		my @to_delete = ();
		foreach my $key (@keys)
		{
				if (is_deleteable($key))
				{
						push @to_delete, $key;
				}
		}
		return @to_delete;
}

sub is_class($)
{
		my $key = shift;
		return is_prefix($key, $class_keyword);
}

my @cached_classes = ();
sub get_all_classes()
{
		my @keys = keys %{$prefix_data{$class_keyword}};
		if (!@cached_classes)
		{
				foreach my $key (@keys)
				{
						if (is_class($key))
						{
								push @cached_classes, $key;
						}
				}
		}

		return @cached_classes;
}

sub is_cmd_class($)
{
		my $key = shift;

		return is_prefix($key, $cmd_class_keyword);
}

my @cached_cmd_classes = ();
sub get_all_cmd_class()
{
		my @keys = get_all_keys();
		if (!@cached_cmd_classes)
		{
				foreach my $key (@keys)
				{
						if (is_cmd_class($key))
						{
								push @cached_cmd_classes, $key;
						}
				}
		}
		return @cached_cmd_classes;
}

sub is_hidden_output_class($)
{
		my $key = shift;

		return is_prefix($key, $hidden_output_class_keyword);
}

my @cached_hidden_output_classes = ();
sub get_all_hidden_output_class()
{
		my @keys = get_all_keys();
		if (!@cached_hidden_output_classes)
		{
				foreach my $key (@keys)
				{
						if (is_hidden_output_class($key))
						{
								push @cached_hidden_output_classes, $key;
						}
				}
		}
		return @cached_hidden_output_classes;
}

sub is_cmd($)
{
		my $key = shift;

		return is_prefix($key, $cmd_keyword);
}

sub is_prop($)
{
		my $key = shift;

		return is_prefix($key, $prop_keyword);
}

sub get_all_props()
{
		my @keys = get_all_keys();
		my %props = ();
		foreach my $key (@keys)
		{
				if (is_prop($key))
				{
						$props{$key} = 1;
				}
		}
		return keys %props;
}

sub prop_is_list($)
{
		my $prop = shift;
		die "$prop not a prop" unless is_prop($prop);
		my $type = get_value($prop, 1);
		if ($type eq $list_keyword)
		{
				return 1;
		}
		elsif ($type eq $scalar_keyword)
		{
				return 0;
		}
		else
		{
				die "Prop $prop must have value $list_keyword or $scalar_keyword\n";
		}
}
sub is_meta_table($)
{
		my $key = shift;

		return is_prefix($key, $meta_table_keyword);
}

sub is_no_custom($)
{
		my $key = shift;

		return is_prefix($key, $no_custom_keyword);
}


sub get_all_cmds(@)
{
		my $class = shift;
		my @keys = get_all_keys();
		my @cmds = ();
		foreach my $key (@keys)
		{
				if (is_cmd($key))
				{
						if (!$class || get_class_level($key) eq $class)
						{
								push @cmds, $key;
						}
				}
		}
		return @cmds;
}

sub is_local($)
{
		my $key = shift;

		return is_prefix($key, $local_keyword);
}

sub is_short($)
{
		my $key = shift;

		return is_prefix($key, $short_keyword);
}


#IDEALLY THIS WOULD BE MOVED INTO A MODULE

sub get_cmd_type(@)
{
		my @args = @_;
		if (scalar @args < 1)
		{
				die "Need type (Args are @args)";
		}
		return $args[0];
}

sub parse_cmd_args(@)
{
		my @args = @_;
		my @mods = ();
		while (index($args[$#args], "=") >= 0 && $args[$#args] =~ /^([^=]*[^\\])=(.*)$/)
		{
				unless ($cmd_mod_keywords{$1})
				{
						die "Error: mod keyword $1 in $args[$#args] is not a valid command mod";
				}
				my $mod = pop @args;
				unshift @mods, [$1, $2];
		}
		return (\@args, \@mods);
}

sub get_cmd_actual_args(@)
{
		my ($args_ref, $mods_ref) = parse_cmd_args(@_);
		return @{$args_ref};
}

sub get_cmd_mods(@)
{
		my ($args_ref, $mods_ref) = parse_cmd_args(@_);
		my @mods = @{$mods_ref};
		return @{$mods_ref};
}


sub get_cmd_if_or_unless_prop_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my @mod_value = ();
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $if_prop_mod_keyword || $mod->[0] eq $unless_prop_mod_keyword)
				{
						push @mod_value, $mod;
				}
		}
		return @mod_value;
}

sub get_cmd_or_if_prop_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $or_if_prop_mod_keyword)
				{
						$mod_value = $mod->[1];
				}
		}
		return $mod_value;
}

sub get_cmd_sep_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $sep_mod_keyword)
				{
						die "Can't have more than one sep" if defined $mod_value;
						$mod_value = $mod->[1];
				}
		}
		return $mod_value;
}

sub get_cmd_optional_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $optional_mod_keyword)
				{
						$mod_value = $mod->[1];
				}
		}
		return $mod_value;
}

sub get_cmd_is_list_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $is_list_mod_keyword)
				{
						$mod_value = $mod->[1];
				}
		}
		return $mod_value;
}

sub get_cmd_all_instances_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $all_instances_mod_keyword)
				{
						$mod_value = $mod->[1];
				}
		}
		return $mod_value;
}

sub get_use_consistent(@)
{
		return get_cmd_all_instances_mod(@_) ? 0 : 1;
}

sub get_cmd_uc_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $uc_mod_keyword)
				{
						$mod_value = $mod->[1];
				}
		}
		return $mod_value;
}

sub get_cmd_limit_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $limit_mod_keyword)
				{
						$mod_value = $mod->[1];
				}
		}
		if (defined $mod_value && $mod_value !~ /^[0-9]+$/)
		{
				$mod_value = undef;
		}
		return $mod_value;
}

sub get_cmd_max_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $max_mod_keyword)
				{
						$mod_value = $mod->[1];
				}
		}
		if (defined $mod_value && $mod_value !~ /^[0-9]+$/)
		{
				$mod_value = undef;
		}
		return $mod_value;
}

sub get_cmd_sort_prop_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $sort_prop_mod_keyword)
				{
						$mod_value = $mod->[1];
				}
		}
		return $mod_value;
}

sub get_cmd_instance_level_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $instance_level_mod_keyword)
				{
						$mod_value = $mod->[1];
				}
		}
		return $mod_value;
}

sub get_cmd_deref_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $deref_mod_keyword)
				{
						die "Deref must be a positive integer" if $mod->[1] !~ /^[0-9]+$/;
						die "Can't have more than one deref value" if defined $mod_value && $mod_value != $mod->[1];
						$mod_value = $mod->[1];
				}
		}
		return $mod_value;
}

sub get_cmd_missing_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $missing_mod_keyword || $mod->[0] eq $missing_prop_mod_keyword || $mod->[0] eq $missing_key_mod_keyword)
				{
						die "Can't have more than one missing value" if defined $mod_value && $mod_value != $mod->[1];
						$mod_value = $mod;
				}
		}
		return $mod_value;
}

sub get_cmd_flatten_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $flatten_mod_keyword)
				{
						die "Can't have more than one flatten value" if defined $mod_value && $mod_value != $mod->[1];
						$mod_value = $mod->[1];
				}
		}
		return $mod_value;
}

sub get_allow_empty_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $allow_empty_mod_keyword)
				{
						die "Can't have more than one allow_empty value" if defined $mod_value && $mod_value != $mod->[1];
						$mod_value = $mod->[1];
				}
		}
		return $mod_value;
}

sub get_prop_is_key_mod(@)
{
		my @mods = get_cmd_mods(@_);
		my $mod_value = undef;
		foreach my $mod (@mods)
		{
				if ($mod->[0] eq $prop_is_key_mod_keyword)
				{
						die "Can't have more than one prop_is_key value" if defined $mod_value && $mod_value != $mod->[1];
						$mod_value = $mod->[1];
				}
		}
		return $mod_value;
}


sub validate_cmd_input_or_output(@)
{
		my @args = get_cmd_actual_args(@_);
		my $type = get_cmd_type(@args);
		if (scalar @args != 3 && scalar @args != 2)
		{
				die "Require 1 or 2 arguments for $type: " . join_cmd_include_args(@args);
		}
		if ($type ne $input_keyword && $type ne $output_keyword)
		{
				die "First arg must be $input_keyword or $output_keyword: " . join_cmd_include_args(@args);
		}
}

sub get_cmd_input_or_output_file_key(@)
{
		my @args = get_cmd_actual_args(@_);
		validate_cmd_input_or_output(@args);
		my $file_key = undef;
		if (scalar @args == 3)
		{
				$file_key = $args[2];
		}
		else
		{
				$file_key = $args[1];
		}
		unless (is_file($file_key))
		{
				die "$file_key is not a file: " . join_cmd_include_args(@args);
		}
		return $file_key;
}

sub get_cmd_input_or_output_prefix(@)
{
		my @args = get_cmd_actual_args(@_);
		validate_cmd_input_or_output(@args);

		if (scalar @args == 3)
		{
				return $args[1];
		}
		else
		{
				return undef;
		}
}

sub is_hidden_cmd_input_or_output(@)
{
		my @args = get_cmd_actual_args(@_);
		validate_cmd_input_or_output(@args);

		return !defined get_cmd_input_or_output_prefix(@args);
}

sub validate_cmd_prop(@)
{
		my @args = get_cmd_actual_args(@_);
		my $type = get_cmd_type(@args);
		if (scalar @args != 3 && scalar @args != 4)
		{
				die "Require 2 or 3 arguments for $type: " . join_cmd_include_args(@args);
		}
		if ($type ne $prop_keyword)
		{
				die "First arg must be $prop_keyword: " . join_cmd_include_args(@args);
		}
}

sub get_cmd_prop_prefix(@)
{
		my @args = get_cmd_actual_args(@_);
		validate_cmd_prop(@args);
		return $args[1];
}

sub get_cmd_prop_class(@)
{
		my @args = get_cmd_actual_args(@_);
		validate_cmd_prop(@args);
		die "Getting prop class: $args[2] is not a class" unless is_class($args[2]);
		return $args[2];
}

sub get_cmd_prop_prop(@)
{
		my @args = get_cmd_actual_args(@_);
		validate_cmd_prop(@args);
		if (scalar @args == 4)
		{
				return $args[3];
		}
		else
		{
				return undef;
		}
}

sub has_cmd_prop_prop(@)
{
		my @args = get_cmd_actual_args(@_);
		return defined get_cmd_prop_prop(@args);
}

sub get_cmd_key_prefix(@)
{
		my @args = get_cmd_actual_args(@_);
		validate_cmd_key(@args);
		return $args[1];
}

sub validate_cmd_key(@)
{
		my @args = get_cmd_actual_args(@_);
		my $type = get_cmd_type(@args);
		if (scalar @args != 3 && scalar @args != 4)
		{
				die "Require 3 or 4 arguments for $type: " . join_cmd_include_args(@args);
		}
		if ($type ne $key_keyword)
		{
				die "First arg must be $key_keyword: " . join_cmd_include_args(@args);
		}
}

sub get_cmd_key_key(@)
{
		my @args = get_cmd_actual_args(@_);
		validate_cmd_key(@args);
		my $key = undef;
		$key = $args[2];
		return $key;
}

sub get_cmd_key_postfix(@)
{
		my @args = get_cmd_actual_args(@_);
		validate_cmd_key(@args);
		if (scalar @args == 4)
		{
				return $args[3];
		}
		else
		{
				return undef;
		}
}

sub has_cmd_key_postfix(@)
{
		my @args = get_cmd_actual_args(@_);
		return defined get_cmd_key_postfix(@args);
}


sub get_cmd_raw_prefix(@)
{
		my @args = get_cmd_actual_args(@_);
		validate_cmd_raw(@args);
		return $args[1];
}

sub validate_cmd_raw(@)
{
		my @args = get_cmd_actual_args(@_);
		my $type = get_cmd_type(@args);
		if (scalar @args != 4)
		{
				die "Require 4 arguments for $type; have " . (scalar @args) . ": " . join_cmd_include_args(@args);
		}
		if ($type ne $raw_keyword)
		{
				die "First arg must be $raw_keyword: " . join_cmd_include_args(@args);
		}
		my $class_level = get_cmd_raw_class_level_no_val(@args);
		if (!is_class($class_level))
		{
				die "Validating raw: $class_level is not a class\n";
		}
}

sub get_cmd_raw_value(@)
{
		my @args = get_cmd_actual_args(@_);
		validate_cmd_raw(@args);
		my $value = undef;
		$value = $args[3];
		return $value;
}

sub get_cmd_raw_class_level_no_val(@)
{
		my @args = get_cmd_actual_args(@_);
		my $value = undef;
		$value = $args[2];
		return $value;
}

sub get_cmd_raw_class_level(@)
{
		my @args = get_cmd_actual_args(@_);
		validate_cmd_raw(@args);
		return get_cmd_raw_class_level_no_val(@args);
}

sub join_cmd_include_args(@)
{
		my @args = get_cmd_actual_args(@_);
		return "$include_prefix\{" . join($multiple_arg_delim, @args) . "\}";
}

my %key_to_includes = ();
my %key_to_includes_internal = ();

sub get_includes_from_value($);
sub get_includes_from_value($)
{
		my $value = shift;
		my $include_to_args = {};
		my $include_to_internal = {};

		my @all_matches = $value =~ /$include_prefix\{([^\{\}]+|[^\{]*(\{[^\{\}]*\}[^\{\}]*)+)\}/g;		
		my @matches = ();
		for (my $i = 0; $i <= $#all_matches; $i+=2)
		{
				push @matches, $all_matches[$i];
		}
		foreach my $match (@matches)
		{
				my @args = ();

				my $cur_match = $match;
				#get the arg delim
				my $cur_arg_delim = $multiple_arg_delim;
				if ($match =~ /^($input_keyword|$output_keyword|$prop_keyword|$key_keyword|$raw_keyword)(.)/)
				{
						$cur_arg_delim = $2;
				}

				while ($cur_match =~ /(.*?(^|[^\\]))$cur_arg_delim(.*)/)
				{
						push @args, $1;
						$cur_match = $3;
				}
				push @args, $cur_match;
				foreach (@args)
				{
						s/\\$cur_arg_delim/$cur_arg_delim/g;
				}

				my $type = get_cmd_type(@args);
				if ($type eq $input_keyword || $type eq $output_keyword)
				{
						validate_cmd_input_or_output(@args);
				}
				elsif ($type eq $prop_keyword)
				{
						validate_cmd_prop(@args);
						#in case prop is an include
						if (has_cmd_prop_prop(@args))
						{
								my $prop_value = get_cmd_prop_prop(@args);
								#in case it had modifiers
								$prop_value =~ s/\\=/=/g;
								my ($prop_include_to_args, $prop_include_to_internal) = get_includes_from_value($prop_value);
								foreach my $cur_include (keys %{$prop_include_to_args})
								{
										my $returned_cur_include = $cur_include;
										#escape them back
										$returned_cur_include =~ s/=/\\=/g;
										$include_to_args->{$returned_cur_include} = $prop_include_to_args->{$cur_include};
										$include_to_internal->{$returned_cur_include} = $prop_include_to_internal->{$cur_include} + 1;
								}
						}
				}
				elsif ($type eq $key_keyword)
				{
						validate_cmd_key(@args);
				}
				elsif ($type eq $raw_keyword)
				{
						validate_cmd_raw(@args);
				}
				else
				{
						die "Bad include type $type in $match in $value\n";
				}

				my $cur_include = "$include_prefix\{$match\}";

				$include_to_args->{$cur_include} = [\@args];
				$include_to_internal->{$cur_include} = 0;
		}
		return ($include_to_args, $include_to_internal);
}

sub get_cmd_includes($)
{
		my $key = shift;

		unless (defined $key_to_includes{$key})
		{
				#need to parse out input, output, props
				if (!is_cmd($key))
				{
						die "$key is not a cmd; can't fetch needed levels";
				}

				my $value = get_value($key, 1);

				($key_to_includes{$key}, undef) = get_includes_from_value($value);
		}

		return %{$key_to_includes{$key}};
}

sub get_cmd_includes_internal($)
{
		my $key = shift;

		unless (defined $key_to_includes_internal{$key})
		{
				#need to parse out input, output, props
				if (!is_cmd($key))
				{
						die "$key is not a cmd; can't fetch needed levels";
				}
				my $value = get_value($key, 1);

				(undef, $key_to_includes_internal{$key}) = get_includes_from_value($value);
		}

		return %{$key_to_includes_internal{$key}};
}

sub get_needed_classes_from_if_value($);
sub get_needed_classes_from_if_value($)
{
		my $text = shift;

		my %needed_classes = ();

		my $return_val = parse_if_value_clauses($text);
		(my $complement, my $do_and, my $clauses, my $is_clause) = @{$return_val};
		my @clauses = @{$clauses};
		my @is_clause = @{$is_clause};
		for (my $i = 0; $i <= $#clauses; $i++)
		{
				my $check_value = $clauses[$i];
				my $has_value = undef;
				if ($is_clause[$i])
				{
						if ($check_value eq $text)
						{
								die "Error parsing clause: $check_value\n";
						}
						my @needed_classes2 = get_needed_classes_from_if_value($check_value);
						map {$needed_classes{$_}=1} @needed_classes2;
				} 
				else
				{
						(my $prop, my $op, my $operand) = (undef, undef, undef);
						eval
						{
								($prop, $op, $operand, undef) = parse_prop_comp($check_value, "", $check_value);
						};
						if ($@)
						{
								if ($@ =~ /^Bad content/)
								{
										print STDERR "Error processing $text\n";
								}
								die $@;
						}

						my @parsed_prop = split(";", $prop);
						if (scalar @parsed_prop == 2)
						{
								$prop = $parsed_prop[0];
						}
						if (is_class($prop))
						{
								$needed_classes{$prop} = 1;
						}
				}
		}
		return keys %needed_classes;
}

sub get_needed_classes_from_if_values($)
{
		my $key = shift;
		my %needed_classes = ();
		eval
		{
				foreach my $do_skip (1, 0)
				{
						my $if_value = undef;
						if ($do_skip && config::has_skip_if_value($key))
						{
								$if_value = config::get_skip_if_value($key);
						}
						elsif (!$do_skip && config::has_run_if_value($key))
						{
								$if_value = config::get_run_if_value($key);
						}
						if (defined $if_value)
						{
								my @cur_needed_classes = get_needed_classes_from_if_value($if_value);
								foreach my $needed_class (@cur_needed_classes)
								{
										$needed_classes{$needed_class} = 0;
								}
						}
				}
		};
		if ($@)
		{
				if ($@ eq main::trap_signal())
				{
						die $@;
				}

				die "Error parsing cmd $key: $@\n";
		}
		return %needed_classes;
}


sub get_needed_classes_for_cmd($)
{
		my $key = shift;
		my $add_for_runif = shift;
		my %needed_classes = ();
		eval
		{
				my %includes = get_cmd_includes($key);
				foreach my $match (keys %includes)
				{
						my @args = @{$includes{$match}->[0]};
						my $type = get_cmd_type(@args);
						my $class_level = undef;
						if ($type eq $input_keyword || $type eq $output_keyword)
						{
								my $file_key = get_cmd_input_or_output_file_key(@args);
								$class_level = get_class_level($file_key);
						}
						elsif ($type eq $prop_keyword)
						{
								$class_level = get_cmd_prop_class(@args);
						}
						elsif ($type eq $key_keyword)
						{
								my $key = get_cmd_key_key(@args);
								$class_level = get_class_level($key);
						}
						elsif ($type eq $raw_keyword)
						{
								$class_level = get_cmd_raw_class_level(@args);
						}
						else
						{
								die "Bad include type $type in $match in $key\n";
						}

						my $all_instances = get_cmd_all_instances_mod(@args);
						if ($all_instances)
						{
								$needed_classes{$class_level} = 1;
						}
						elsif (!exists $needed_classes{$class_level})
						{
								$needed_classes{$class_level} = 0;
						}
				}
				if ($add_for_runif)
				{
						my %needed_classes2 = config::get_needed_classes_from_if_values($key);
						map {$needed_classes{$_}=$needed_classes2{$_}} keys %needed_classes2;
				}

		};
		if ($@)
		{
				if ($@ eq main::trap_signal())
				{
						die $@;
				}

				die "Error parsing cmd $key: $@\n";
		}
		return %needed_classes;
}

sub get_input_or_output_keys_for_cmd(@)
{
		my $key = shift;
		my $in_or_out = shift;
		my $return_is_list = shift;
		my %return_val  = ();
		eval
		{
				my %includes = get_cmd_includes($key);
				foreach my $match (keys %includes)
				{
						my @args = @{$includes{$match}->[0]};
						my $type = get_cmd_type(@args);
						if ($type eq $in_or_out)
						{
								if ($return_is_list)
								{
										my $is_list = get_cmd_is_list_mod(@args);										
										$return_val{get_cmd_input_or_output_file_key(@args)} = $is_list;
								}
								else
								{
										my $is_optional = get_cmd_optional_mod(@args);
										$return_val{get_cmd_input_or_output_file_key(@args)} = $is_optional;
								}
						}
				}
		};
		if ($@)
		{
				if ($@ eq main::trap_signal())
				{
						die $@;
				}

				die "Can't parse cmd $key: $@\n";
		}
		return %return_val;
}

sub get_input_keys_for_cmd($)
{
		my $key = shift;
		my $return_is_list = shift;
		return get_input_or_output_keys_for_cmd($key, $input_keyword, $return_is_list);
}

sub get_output_keys_for_cmd($)
{
		my $key = shift;
		my $return_is_list = shift;
		return get_input_or_output_keys_for_cmd($key, $output_keyword, $return_is_list);
}

sub get_inputs_or_outputs_for_cmd(@)
{
		my $key = shift;
		#hash with key class, value array of instances
		my $class_to_instances = shift;
		#hash with key instance, value hash of args 
		my $instance_to_args = shift;
		#function to convert an instance to an object
		my $instance_to_object = shift;

		my $in_or_out = shift;
		my $query_mode = shift;

		my %in_or_outs = ();

		my $query_result = {};
		my $good_query = 1;

		eval
		{
				my %includes = get_cmd_includes($key);
				my $cmd_class_level = get_class_level($key);
				foreach my $match (keys %includes)
				{
						my @args = @{$includes{$match}->[0]};

						my $type = get_cmd_type(@args);
						if ($type eq $in_or_out)
						{
								my $file_key = get_cmd_input_or_output_file_key(@args);
								my $base_class = $cmd_class_level;
								my $class = get_class_level($file_key);

								#OLD
								#my %base_ancestors = get_ancestor_keys($base_class);
								#if (exists $base_ancestors{$class})
								#{
								#		$class = $base_class;
								#}

								#NEW
								my %base_ancestors = get_ancestor_keys($class);
								#if the cmd class level is an ancestor of the file level
								if (exists $base_ancestors{$cmd_class_level})
								{
											$base_class = $class;
								}
								my %cmd_class_ancestors = get_ancestor_keys($cmd_class_level);
								if (exists $cmd_class_ancestors{$class})
								{
										$class = $cmd_class_level;
								}
								my $instance_level = get_cmd_instance_level_mod(@args);
								if (defined $instance_level)
								{
										$class = $instance_level;
								}

								my $class_instances = filter_instances($class_to_instances, $instance_to_args, $class, $base_class, $query_mode, @args);
								if ($query_mode)
								{
										#have the result of the query, so return
										my $cur_good_query = $class_instances->[0];
										$class_instances = $class_instances->[1];
										if (!$cur_good_query)
										{
												$good_query = 0;
										}
										foreach my $cur_class (keys %{$class_instances})
										{
												foreach my $instance (@{$class_instances->{$cur_class}})
												{
														$query_result->{$cur_class}->{$instance} = 1;
												}
										}
										foreach my $file_arg (get_args($file_key, 1))
										{
												$query_result->{$class}->{$file_arg} = 1;														
										}
								}
								else
								{
										my $use_consistent = get_use_consistent(@args);
										foreach my $instance (@{$class_instances})
										{
												push @{$in_or_outs{$file_key}}, \%{$instance_to_args->{$use_consistent}->{$instance}};
										}
								}
						}
				}
		};
		if ($@)
		{
				if ($@ eq main::trap_signal())
				{
						die $@;
				}
				die "Can't parse cmd $key: $@\n";
		}

		if ($query_mode)
		{
				return [$good_query, $query_result];
		}
		else
		{
				return %in_or_outs;
		}
}

sub get_inputs_for_cmd(@) 
{
		my $key = shift;
		#hash with key class, value array of instances
		my $class_to_instances = shift;
		#hash with key instance, value hash of args 
		my $instance_to_args = shift;
		#function to convert an instance to an object
		my $instance_to_object = shift;
		my $query_mode = shift;

		return get_inputs_or_outputs_for_cmd($key, $class_to_instances, $instance_to_args, $instance_to_object, $input_keyword, $query_mode);
}

sub parse_prop_comp($$$)
{
		my $content = shift;
		my $prop_filter1 = shift;
		my $prop_filter2 = shift;

		my @content = split($vector_arg_delim, $content, -1);
		if (scalar @content < 1)
		{
				die "Bad content $prop_filter2 for $prop_filter1: value is empty in $content";
		}
		my $prop = shift @content;
		my $op = undef;
		my $original_operand = undef;
		my $deref = 0;

		while (@content)
		{
				if (scalar @content < 1)
				{
						die "Error: modifiers for $prop_filter1=$prop_filter2 must all be of length greater than 0\n";
				}
				my $first = shift @content;
				if ($valid_prop_ops{$first})
				{
						die "Can't have more than one operation for $prop_filter1" if defined $op;
						$op = $first;
						if ($valid_prop_ops{$first} > 1)
						{
								my $second = shift @content;
								$original_operand = $second;
						}
						else
						{
								$original_operand = 1;
						}
				}
				elsif ($first eq $deref_mod_keyword)
				{
						my $second = shift @content;
						$deref = $second;
				}
				else
				{
						die "Unrecognized modifier $first for $prop_filter1\n";
				}
		}

		return ($prop, $op, $original_operand, $deref);
}

sub check_prop_comp($$$)
{
		my $value = shift;
		my $op = shift;
		my $operand = shift;

		my $cur_result = undef;
		if ($op eq $def_keyword)
		{
				$cur_result = (defined $value);
		}
		elsif (defined $value)
		{
				local $SIG{__WARN__} = sub { die $_[0] };
				if ($op eq $eq_keyword)
				{
						my $new_value = undef;
						eval
						{
								$new_value = ($value == $operand);
						};
						if ($@)
						{
								if ($@ eq main::trap_signal())
								{
										die $@;
								}

								$new_value = ($value eq $operand);
						}
						$cur_result = $new_value;
				}
				elsif ($op eq $ne_keyword)
				{
						my $new_value = undef;
						eval
						{
								$new_value = ($value != $operand);
						};
						if ($@)
						{
								if ($@ eq main::trap_signal())
								{
										die $@;
								}

								$new_value = ($value ne $operand);
						}
						$cur_result = $new_value;
				}
				elsif ($op eq $lt_keyword)
				{
						$cur_result = ($value < $operand);
				}
				elsif ($op eq $gt_keyword)
				{
						$cur_result = ($value > $operand);
				}
				elsif ($op eq $le_keyword)
				{
						$cur_result = ($value <= $operand);
				}
				elsif ($op eq $ge_keyword)
				{
						$cur_result = ($value >= $operand);
				}
				elsif ($op eq $cmplt_keyword)
				{
						$cur_result = ($value cmp $operand < 0);
				}
				elsif ($op eq $cmpgt_keyword)
				{
						$cur_result = ($value cmp $operand > 0);
				}
				elsif ($op eq $cmple_keyword)
				{
						$cur_result = ($value cmp $operand <= 0);
				}
				elsif ($op eq $cmpge_keyword)
				{
						$cur_result = ($value cmp $operand) >= 0;
				}
		}
		return $cur_result;
}

sub get_outputs_for_cmd(@) 
{
		my $key = shift;

		#hash with key class, value array of instances
		my $class_to_instances = shift;
		#hash with key instance, value hash of args 
		my $instance_to_args = shift;
		#function to convert an instance to an object
		my $instance_to_object = shift;
		my $query_mode = shift;

		return get_inputs_or_outputs_for_cmd($key, $class_to_instances, $instance_to_args, $instance_to_object, $output_keyword, $query_mode);
}

sub filter_instances($$$@)
{
		my $class_to_instances = shift;
		#pass undef to get a list of needed properties
		my $instance_to_args = shift;
		#used to get the original instances that we will filter
		my $class = shift;
		#used to get the potential instances that will define properties that will be used to filter
		my $base_class = shift;
		my $query_mode = shift;

		my @args = @_;
		my $use_consistent = get_use_consistent(@args);

		local $SIG{__WARN__} = sub { die $_[0] };

		my $class_instances = undef;
		if (defined $class_to_instances)
		{

				foreach my $jason (keys %{$class_to_instances->{$use_consistent}})
				{
						my $adam = $class_to_instances->{$use_consistent}->{$jason};
				}

				$class_instances = $class_to_instances->{$use_consistent}->{$class};
		}

		my $sort_prop_mod = get_cmd_sort_prop_mod(@args);

		my $sort_instances_helper = sub
		{
				my @instances = @_;

				my %sv = ();
				foreach my $instance (@instances)
				{
						if (exists $instance_to_args->{$use_consistent}->{$instance}{$sort_prop_mod})
						{
								$sv{$instance} = $instance_to_args->{$use_consistent}->{$instance}{$sort_prop_mod};
						}
				}

				my $max_sort = undef;
				foreach my $instance (keys %sv)
				{
						$max_sort = $sv{$instance} unless defined $max_sort && ($max_sort cmp $sv{$instance}) > 0;
				}
				$max_sort = 1 unless defined $max_sort;
				foreach my $instance (@instances)
				{
						unless (exists $sv{$instance})
						{
								$sv{$instance} = $max_sort;
						}
				}

				my $all_numeric = 1;
				foreach my $instance (@instances)
				{
						if (!looks_like_number($sv{$instance}))
						{
								$all_numeric = 0;
								last;
						}
				}

				if ($all_numeric)
				{
						@instances = sort {$sv{$a} <=> $sv{$b}} @instances;
				}
				else
				{
						@instances = sort {$sv{$a} cmp $sv{$b}} @instances;
				}
				return @instances;
		};

		if ($class_instances || $query_mode)
		{
				#check for filters
				my @prop_filters = get_cmd_if_or_unless_prop_mod(@args);
				my $or_if_prop = get_cmd_or_if_prop_mod(@args);
				my $limit_mod = get_cmd_limit_mod(@args);
				my $max_mod = get_cmd_max_mod(@args);

				if (!@prop_filters)
				{
						if ($query_mode)
						{
								my %to_return = ();
								if ($sort_prop_mod)
								{
										push @{$to_return{$class}}, $sort_prop_mod;
								}
								return ["good", \%to_return];
						}
						else
						{
								my @to_return = @{$class_instances};
								if (defined $sort_prop_mod)
								{
										@to_return = $sort_instances_helper->(@to_return);

								}
								if ($max_mod && $max_mod > 0)
								{
										if (scalar @to_return > $max_mod)
										{
												die "Error: returned more than $max_mod instance\n";
										}
								}

								if ($limit_mod && $limit_mod > 0 && scalar @to_return)
								{
										@to_return = @to_return[0..($limit_mod-1)];
								}
								$class_instances = \@to_return;
								return $class_instances;
						}

				}

				my @results = ();
				my $to_return_query_mode = {};
				if ($sort_prop_mod)
				{
						$to_return_query_mode->{$class}->{$sort_prop_mod} = 1;
				}
				my $good_query = 1;
				foreach my $prop_filter (@prop_filters)
				{
						if ($prop_filter)
						{
								#now try to parse it
								my $complement_result = $prop_filter->[0] eq $unless_prop_mod_keyword;
								my $content = $prop_filter->[1];

								(my $prop, my $op, my $original_operand, my $deref) = parse_prop_comp($content, $prop_filter->[0], $prop_filter->[1]);

								if ($query_mode)
								{
										#in this case it was a query to get the relevant props
										my @to_return = ();
										my @to_return_base = ();
										if (defined $original_operand && substr($original_operand, 0, 1) eq "@")
										{
												push @to_return_base, substr($original_operand, 1);
										}

										push @to_return, $prop;
										
										if ($deref > 0)
										{
												if (!defined $class_instances)
												{
														$good_query = 0;
												}
										}
										map {$to_return_query_mode->{$class}->{$_} = 1} @to_return;

										#WARNING: UNSURE
										#Will this break anything
										#my $base_class_to_use = $use_consistent ? $class : $base_class;
										my $base_class_to_use = $base_class;

										map {$to_return_query_mode->{$base_class_to_use}->{$_} = 1} @to_return_base;
								}
								if (defined $class_instances)
								{
										for (my $j = 0; $j <= $#{$class_instances}; $j++)
										{
												if ($j <= $#results)
												{
														next if $or_if_prop && $results[$j];
														next if !$or_if_prop && !$results[$j];
												}

												my $instance = $class_instances->[$j];

												my @operand = ();
												if (defined $original_operand)
												{
														@operand = ($original_operand);

														if (substr($original_operand, 0, 1) eq "@")
														{
																@operand = ();
																my $operand = substr($original_operand, 1);

																#WARNING: UNSURE
																#used to only pull from base class if had use_consistent != 0

																#my @instances_to_check = ($instance);
																my @instances_to_check = @{$class_to_instances->{1}->{$base_class}};

																if ($use_consistent)
																{
																		@instances_to_check = @{$class_to_instances->{$use_consistent}->{$base_class}};
																}
																#my @instances_to_check = @{$class_to_instances->{$use_consistent}->{$base_class}};
																
																foreach my $instance_to_check (@instances_to_check)
																{
																		my $cur_value = $instance_to_args->{1}->{$instance_to_check}{$operand};
																		if (defined $cur_value)
																		{
																				if (ref($cur_value) && lc(ref($cur_value)) eq "array")
																				{
																						push @operand, @{$cur_value};
																				}
																				else
																				{
																						push @operand, $cur_value;
																				}
																		}

																}
														}
												}


												my @values = ($prop);
												for (my $i = 0; $i <= $deref; $i++)
												{
														if ($query_mode)
														{
																foreach my $value (@values)
																{
																		$to_return_query_mode->{$class}->{$value} = 1 if defined $value;
																		$good_query = 0 unless defined $value;
																}
														}

														my @new_values = ();
														foreach my $value (@values)
														{
																if (defined $value)
																{
																		my $cur_value = $instance_to_args->{$use_consistent}->{$instance}{$value};

																		if (defined $cur_value && ref($cur_value) && lc(ref($cur_value)) eq "array")
																		{
																				
																				push @new_values, @{$cur_value};
																		}
																		else
																		{
																				push @new_values, $cur_value;
																		}
																}
																else
																{
																		push @new_values, $value;
																}
														}
														@values = @new_values;
												}
												next if $query_mode;

												if (defined $op && defined $original_operand)
												{
														for (my $i = 0; $i <= $#values; $i++)
														{
																my $cur_result = 0;
																foreach my $operand (@operand)
																{
																		$cur_result = check_prop_comp($values[$i], $op, $operand);
																		if ($cur_result)
																		{
																				#or semantics; once we see a true value we quit
																				last;
																		}
																}
																$values[$i] = $cur_result;
														}
												}
												my $result = 0;
												foreach my $value (@values)
												{
														$result = 1 if $value;
														last if $result;
												}
												if ($complement_result)
												{
														$result = !$result;
												}

												if ($j > $#results)
												{
														$results[$j] = $result;
												}
												else
												{
														#intersect
														if ($or_if_prop)
														{
																$results[$j] = $results[$j] || $result;
														}
														else
														{
																$results[$j] = $results[$j] && $result;
														}
												}
										}
								}
						}
				}

				if ($query_mode)
				{
						my $new_return_query_mode = {};
						foreach my $cur_class (keys %{$to_return_query_mode})
						{
								@{$new_return_query_mode->{$cur_class}} = keys %{$to_return_query_mode->{$cur_class}};
						}
						return [$good_query, $new_return_query_mode];
				}
				my @new_class_instances = ();
				for (my $j = 0; $j <= $#{$class_instances}; $j++)
				{
						if ($results[$j])
						{
								push @new_class_instances, $class_instances->[$j];
						}
				}
				if (defined $sort_prop_mod)
				{
						@new_class_instances = $sort_instances_helper->(@new_class_instances);
				}
				if ($max_mod && $max_mod > 0 && scalar @new_class_instances > $max_mod)
				{
						die "Error: returned more than $max_mod instances\n";
				}

				if ($limit_mod && $limit_mod > 0 && scalar @new_class_instances >= $limit_mod)
				{
						@new_class_instances = @new_class_instances[0..($limit_mod-1)];
				}

				$class_instances = \@new_class_instances;
		}
		return $class_instances;
}

sub bind_instances_to_value($$$$@);
sub bind_instances_to_cmd($@)
{
		my $key = shift;

		#hash with key class, value array of instances
		my $class_to_instances = shift;
		#hash with key instance, value hash of args 
		my $instance_to_args = shift;
		#function to convert an instance to an object
		my $instance_to_object = shift;

		my $query_mode = shift;

		#leaf class to base multiline expansion for list properties
		my $multiline_for_list_props = shift || "";

		my $cmd = get_value($key, 1);

		my $cmd_class_level = get_class_level($key);
		my $return_value = undef;

		eval
		{
				my %includes = get_cmd_includes($key);
				my %includes_internal = get_cmd_includes_internal($key);
				$return_value = bind_instances_to_value($cmd, $cmd_class_level, \%includes, \%includes_internal, $class_to_instances, $instance_to_args, $instance_to_object, $query_mode, $multiline_for_list_props, $key);
		};
		if ($@)
		{
				if ($@ eq main::trap_signal())
				{
						die $@;
				}

				die "Error binding cmd $key: $@\n";
		}
		return $return_value;
}

my %bind_cache = ();
#NOTE: lines involving $bind_cache are simply shortcuts to return cache value if present, else compute and store
sub bind_instances_to_value($$$$@)
{
		my $cmd = shift;
		my $cmd_class_level = shift;
		my $includes = shift;
		my %includes = %{$includes};
		my $includes_internal = shift;
		my %includes_internal = %{$includes_internal};

		#hash with key class, value array of instances
		my $class_to_instances = shift;
		#hash with key instance, value hash of args 
		my $instance_to_args = shift;
		#function to convert an instance to an object
		my $instance_to_object = shift;

		my $query_mode = shift;

		#leaf class to base multiline expansion for list properties
		my $multiline_for_list_props = shift || "";
		
		my $query_result = {};
		my $good_query = 1;

		my $cmd_key = shift || "";

		my @cmd_texts = ($cmd);

		my %expanded_something = ();

		if (scalar keys %includes == 0)
		{
				$expanded_something{$multiline_for_list_props} = 1;
		}
		my $match_num = 0;
		foreach my $match (sort {$includes_internal{$a} == $includes_internal{$b} ? length($b) <=> length($a) : $includes_internal{$a} <=> $includes_internal{$b}} keys %includes)
		{
				$match_num++;
				my $to_replace = $match;

				$to_replace = quotemeta($to_replace);
				#$to_replace =~ s/\\/\\\\/g;
				#$to_replace =~ s/\^/\\\^/g;
				#$to_replace =~ s/\$/\\\$/g;
				#$to_replace =~ s/\|/\\\|/g;
				#$to_replace =~ s/\+/\\\+/g;


				my @args = @{$includes{$match}->[0]};

				my $allow_empty = $cmd_key && defined $bind_cache{$cmd_key}{$match_num} ? $bind_cache{$cmd_key}{$match_num}{get_allow_empty_mod} : $bind_cache{$cmd_key}{$match_num}{get_allow_empty_mod} = get_allow_empty_mod(@args);

				my $prop_is_key = $cmd_key && defined $bind_cache{$cmd_key}{$match_num} ? $bind_cache{$cmd_key}{$match_num}{get_prop_is_key_mod} : $bind_cache{$cmd_key}{$match_num}{get_prop_is_key_mod} = get_prop_is_key_mod(@args);

				my $type = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_type} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_type} : $bind_cache{$cmd_key}{$match_num}{get_cmd_type} = get_cmd_type(@args);
				my @replacements = ("");

				my $sep = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_sep_mod} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_sep_mod} : $bind_cache{$cmd_key}{$match_num}{get_cmd_sep_mod} = get_cmd_sep_mod(@args);
				$sep = " " unless defined $sep;
				my $expanded_class = undef;

				my $is_uc = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_uc_mod} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_uc_mod} : $bind_cache{$cmd_key}{$match_num}{get_cmd_uc_mod} = get_cmd_uc_mod(@args);

				my $use_consistent = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_use_consistent} ? $bind_cache{$cmd_key}{$match_num}{get_use_consistent} : $bind_cache{$cmd_key}{$match_num}{get_use_consistent} = get_use_consistent(@args);

				if ($type eq $input_keyword || $type eq $output_keyword)
				{
						my $is_hidden = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{is_hidden_cmd_input_or_output} ? $bind_cache{$cmd_key}{$match_num}{is_hidden_cmd_input_or_output} : $bind_cache{$cmd_key}{$match_num}{is_hidden_cmd_input_or_output} = is_hidden_cmd_input_or_output(@args);
						unless ($is_hidden)
						{
								my $prefix = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_input_or_output_prefix} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_input_or_output_prefix} : $bind_cache{$cmd_key}{$match_num}{get_cmd_input_or_output_prefix} = get_cmd_input_or_output_prefix(@args);

								my $file_key = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_input_or_output_file_key} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_input_or_output_file_key} : $bind_cache{$cmd_key}{$match_num}{get_cmd_input_or_output_file_key} = get_cmd_input_or_output_file_key(@args);
								my $class = get_class_level($file_key);
								$expanded_class = $class;

								##FIXME
								##Should this be here?

								#If the file is at a child level, then when comparing something like if_prop=p:eq:@q
								#we want to draw @q from the child level
								#e.g. !{input,,burden_test_file,if_prop=test:eq:@test,all_instances=1} means we want to get all burden tests (not just our children)
								#as long as they match a burden test under us
								#thus, set base_class to the child, since that is where @test will be drawn from
								my %base_ancestors = get_ancestor_keys($class);
								my $base_class = $cmd_class_level;
								#if the cmd class level is an ancestor of the file level

								if (exists $base_ancestors{$cmd_class_level})
								{

										$base_class = $class;
								}

								#If the file is at a parent level, then want to draw instances from this level
								#all props will be inherited, but when overriden we want to use the child (this level) value
								my %cmd_class_ancestors = get_ancestor_keys($cmd_class_level);
								if (exists $cmd_class_ancestors{$class})
								{
										$class = $cmd_class_level;
								}

								#if hardcoded what level to draw classes from, then use that
								my $instance_level = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_instance_level_mod} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_instance_level_mod} : $bind_cache{$cmd_key}{$match_num}{get_cmd_indstance_level_mod} = get_cmd_instance_level_mod(@args);

								if (defined $instance_level)
								{
										$class = $instance_level;
								}

								my $class_instances = filter_instances($class_to_instances, $instance_to_args, $class, $base_class, $query_mode, @args);

								if ($query_mode)
								{
										my $cur_good_query = $class_instances->[0];
										$class_instances = $class_instances->[1];
										$good_query = 0 unless $cur_good_query;

										foreach my $cur_class (keys %{$class_instances})
										{
												foreach my $instance (@{$class_instances->{$cur_class}})
												{
														$query_result->{$cur_class}->{$instance} = 1;
												}
										}
										foreach my $file_arg (get_args($file_key, 1))
										{
												$query_result->{$class}->{$file_arg} = 1;														
										}
								}
								else
								{
										die "No instances for $class in $match" unless ($allow_empty || ($class_instances && @{$class_instances}));

										foreach my $instance (@{$class_instances})
										{
												die "No instance" unless defined $instance;
												die "No args for $instance" unless $instance_to_args->{$use_consistent}->{$instance};
												$replacements[0] .= $sep if $replacements[0] ne "";
												$replacements[0] .= $prefix;
												$replacements[0] .= $sep if $prefix ne "" && $prefix !~ /=$/;
												$replacements[0] .= get_path($file_key, %{$instance_to_args->{$use_consistent}->{$instance}});
										}
								}
						}
						else
						{
								$to_replace = '\s*' . "$to_replace";
						}
				}
				elsif ($type eq $prop_keyword)
				{

						my $class = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_prop_class} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_prop_class} : $bind_cache{$cmd_key}{$match_num}{get_cmd_prop_class} = get_cmd_prop_class(@args);
						$expanded_class = $class;
						my $prefix = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_prop_prefix} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_prop_prefix} : $bind_cache{$cmd_key}{$match_num}{get_cmd_prop_prefix} = get_cmd_prop_prefix(@args);

						my $class_instances = filter_instances($class_to_instances, $instance_to_args, $class, $cmd_class_level, $query_mode, @args);

						my $has_prop_prop = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{has_cmd_prop_prop} ? $bind_cache{$cmd_key}{$match_num}{has_cmd_prop_prop} : $bind_cache{$cmd_key}{$match_num}{has_cmd_prop_prop} = has_cmd_prop_prop(@args);
						my $prop = undef;
						if ($has_prop_prop)
						{
								$prop = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_prop_prop} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_prop_prop} : $bind_cache{$cmd_key}{$match_num}{get_cmd_prop_prop} = get_cmd_prop_prop(@args);
						}

						my $deref = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_deref_mod} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_deref_mod} : $bind_cache{$cmd_key}{$match_num}{get_cmd_deref_mod} = get_cmd_deref_mod(@args);
						$deref = 0 unless $deref;
						my $missing = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_missing_mod} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_missing_mod} : $bind_cache{$cmd_key}{$match_num}{get_cmd_missing_mod} = get_cmd_missing_mod(@args);
						my $flatten = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_flatten_mod} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_flatten_mod} : $bind_cache{$cmd_key}{$match_num}{get_cmd_flatten_mod} = get_cmd_flatten_mod(@args);

						my $has_an_include = 0;
						foreach my $cur_include (map {quotemeta($_)} keys %includes)
						{
								if (defined $prop && $prop =~ /$cur_include/)
								{
										$has_an_include = 1;
										last;
								}
						}

						if ($query_mode)
						{
								my $cur_good_query = $class_instances->[0];
								$class_instances = $class_instances->[1];

								$good_query = 0 unless $cur_good_query;


								foreach my $cur_class (keys %{$class_instances})
								{
										foreach my $instance (@{$class_instances->{$cur_class}})
										{
												$query_result->{$cur_class}->{$instance} = 1;
										}
								}
								if ($has_prop_prop && !$has_an_include)
								{
										$query_result->{$class}->{$prop} = 1;
								}
								else
								{
										$query_result->{$class}->{$class} = 1;
								}
								if (defined $missing && $missing->[0] eq $missing_prop_mod_keyword)
								{
										$query_result->{$class}->{$missing->[1]} = 1;
								}

								if ($prop_is_key)
								{
										foreach my $prop_arg (get_args($prop, has_dir_key($prop)))
										{
												$query_result->{$class}->{$prop_arg} = 1;														
										}
								}
								#now need to fetch the instances
								$class_instances = filter_instances($class_to_instances, $instance_to_args, $class, $cmd_class_level, 0, @args);
								if ($has_an_include || $deref > 0)
								{
										$good_query = 0 unless $class_instances;
								}
						}

						if (!$query_mode || ($query_mode && ($has_an_include || $deref > 0) && $class_instances && @{$class_instances}))
						{
								die "No instances for $class in $match" unless $allow_empty || ($class_instances && @{$class_instances});

							INSTANCES:
								foreach my $instance (@{$class_instances})
								{
										die "No args for $instance" unless $instance_to_args->{$use_consistent}->{$instance} || $query_mode;
										my @prop_values = ();

										my $ref_level = 0;
										my $prop_value = &$instance_to_object($instance);

										if ($has_prop_prop)
										{
												do
												{
														if ($ref_level == 0)
														{
																if ($has_an_include)
																{
																		my %cur_includes = %{$includes};
																		delete $cur_includes{$match};

																		my $query_prop = bind_instances_to_value($prop, $cmd_class_level, \%cur_includes, $includes_internal, $class_to_instances, $instance_to_args, $instance_to_object, $query_mode, $multiline_for_list_props, $prop);
																		if ($query_mode)
																		{
																				my $cur_good_query = $query_prop->[0];
																				$good_query = 0 unless $cur_good_query;
																				my $needed_prop = $query_prop->[1];

																				foreach my $cur_class (keys %{$needed_prop})
																				{
																						foreach my $cur_prop (keys %{$needed_prop->{$cur_class}})
																						{
																								$query_result->{$cur_class}->{$cur_prop} = 1;
																						}
																						#now try to actually fetch the prop value
																						eval
																						{
																								$prop = bind_instances_to_value($prop, $cmd_class_level, \%cur_includes, $includes_internal, $class_to_instances, $instance_to_args, $instance_to_object, 0, $multiline_for_list_props, $prop);
																						};
																						if ($@ || !defined $prop || $prop eq "")
																						{
																								$good_query = 0;
																								next INSTANCES;
																						}
																						else
																						{
																								$query_result->{$cur_class}->{$prop} = 1;
																						}
																				}

																		}
																		else
																		{
																				$prop = $query_prop;
																		}
																}

																@prop_values = ($prop);
														}
														if ($query_mode)
														{
																my $good_to_continue = 1;
																foreach my $value (@prop_values)
																{
																		$query_result->{$class}->{$value} = 1 if defined $value;
																		$good_query = 0 unless defined $value;
																		$good_to_continue = 0;
																}
																if (!$good_to_continue)
																{
																		next INSTANCES;
																}
														}

														my @new_prop_values = ();
														foreach my $cur_prop (@prop_values)
														{
										
																if (!exists $instance_to_args->{$use_consistent}->{$instance}{$cur_prop})
																{
																		#in case the property was a class
																		if (exists $class_to_instances->{$use_consistent}->{$cur_prop})
																		{
																				$prop_value = $class_to_instances->{$use_consistent}->{$cur_prop};
																		}
																		else
																		{
																				if (defined $missing)
																				{
																						$prop_value = $missing->[1];
																						if ($missing->[0] eq $missing_key_mod_keyword)
																						{
																								if ($query_mode && !exists $instance_to_args->{$use_consistent}->{$instance})
																								{
																										$good_query = 0;
																										next INSTANCES;
																								}

																								eval
																								{
																										if ($query_mode)
																										{
																												foreach my $prop_arg (get_args($prop_value, has_dir_key($prop_value)))
																												{
																														$query_result->{$class}->{$prop_arg} = 1;
																												}
																										}

																										$prop_value = get_value($prop_value, %{$instance_to_args->{$use_consistent}->{$instance}});
																								};
																								if ($@)
																								{
																										if ($query_mode)
																										{
																												$good_query = 0;
																												next INSTANCES;
																										}
																										else
																										{
																												die $@;
																										}
																								}
																						}
																						elsif ($missing->[0] eq $missing_prop_mod_keyword)
																						{
																								if (exists $instance_to_args->{$use_consistent}->{$instance}{$prop_value})
																								{
																										$prop_value = $instance_to_args->{$use_consistent}->{$instance}{$prop_value};
																								}
																								elsif (exists $class_to_instances->{$use_consistent}->{$prop_value})
																								{
																										$prop_value = $class_to_instances->{$use_consistent}->{$prop_value}
																								}
																								else
																								{
																										if ($query_mode)
																										{
																												$good_query = 0;
																												next INSTANCES;
																										}
																										die "No missing prop $prop_value for $instance\n"
																								}
																						}
																				}
																				else
																				{
																						if ($query_mode)
																						{
																								$good_query = 0;
																								next INSTANCES;
																						}
																						die "No prop $cur_prop for $instance\n";

																				}
																		}
																}
																else
																{
																		$prop_value = $instance_to_args->{$use_consistent}->{$instance}{$cur_prop};
																}

																if (ref($prop_value) && ref($prop_value) eq "ARRAY")
																{
																		push @new_prop_values, @{$prop_value};
																}
																else
																{
																		push @new_prop_values, $prop_value;
																}
														}
														@prop_values = @new_prop_values;
														$ref_level++;
												} 
												while ($ref_level <= $deref)
										}
										else
										{
												#pass in the id of the object
												@prop_values = ($prop_value);
										}

										if ($multiline_for_list_props && !$flatten)
										{
												@replacements = ();
										}

										if ($is_uc)
										{
												@prop_values = map {uc} @prop_values;
										}

										if ($prop_is_key)
										{
												if ($query_mode)
												{
														foreach my $cur_prop (@prop_values)
														{
																if (is_key($cur_prop))
																{
																		foreach my $prop_arg (get_args($cur_prop, has_dir_key($cur_prop)))
																		{
																				$query_result->{$class}->{$prop_arg} = 1;														
																		}
																}
														}
												}
												else
												{
														@prop_values = map {get_value($_, %{$instance_to_args->{$use_consistent}->{$instance}})} @prop_values;
												}
										}

										foreach my $cur_prop_value (@prop_values)
										{
												if ($multiline_for_list_props && !$flatten)
												{
														push @replacements, "";
												}
												$replacements[$#replacements] .= $sep if $replacements[$#replacements] ne "";
												$replacements[$#replacements] .= $prefix;
												$replacements[$#replacements] .= $sep if $prefix ne "" && $prefix !~ /=$/;
												$replacements[$#replacements] .= $cur_prop_value;
										}
								}
						}
				}
				elsif ($type eq $key_keyword)
				{
						my $prefix = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_key_prefix} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_key_prefix} : $bind_cache{$cmd_key}{$match_num}{get_cmd_key_prefix} = get_cmd_key_prefix(@args);
						my $key = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_key_key} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_key_key} : $bind_cache{$cmd_key}{$match_num}{get_cmd_key_key} = get_cmd_key_key(@args);
						my $class = get_class_level($key);
						$expanded_class = $class;

						##See comments at $input_keyword section for explanation of these settings
						my %base_ancestors = get_ancestor_keys($class);
						my $base_class = $cmd_class_level;
						if (exists $base_ancestors{$cmd_class_level})
						{
					  			$base_class = $class;
						}
						my %cmd_class_ancestors = get_ancestor_keys($cmd_class_level);
						if (exists $cmd_class_ancestors{$class})
						{
								$class = $cmd_class_level;
						}

						my $class_instances = filter_instances($class_to_instances, $instance_to_args, $class, $base_class, $query_mode, @args);
						my $lookup_key = $key;
						my $has_postfix = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{has_cmd_key_postfix} ? $bind_cache{$cmd_key}{$match_num}{has_cmd_key_postfix} : $bind_cache{$cmd_key}{$match_num}{has_cmd_key_postfix} = has_cmd_key_postfix(@args);
						if ($has_postfix)
						{
								my $cmd_key_postfix = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_key_postfix} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_key_postfix} : $bind_cache{$cmd_key}{$match_num}{get_cmd_key_postfix} = get_cmd_key_postfix(@args);
								$lookup_key = get_postfix_key($key, $cmd_key_postfix);
						}


						if ($query_mode)
						{
								my $cur_good_query = $class_instances->[0];
								$class_instances = $class_instances->[1];
								$good_query = 0 unless $cur_good_query;
								foreach my $cur_class (keys %{$class_instances})
								{
										foreach my $instance (@{$class_instances->{$cur_class}})
										{
												$query_result->{$cur_class}->{$instance} = 1;
										}
								}
								my @key_args = ();
								if (is_key($lookup_key))
								{
										@key_args = get_args($lookup_key)
								}
								else
								{
										@key_args = util::get_args($lookup_key);
								}
								foreach my $key_arg (@key_args)
								{
										$query_result->{$class}->{$key_arg} = 1;														
								}
						}
						else
						{
								die "No instances for $class in $match" unless $allow_empty || ($class_instances && @{$class_instances});

								foreach my $instance (@{$class_instances})
								{
										die "No args for $instance" unless $instance_to_args->{$use_consistent}->{$instance};
										$replacements[0] .= $sep if $replacements[0] ne "";
										$replacements[0] .= $prefix;
										$replacements[0] .= $sep if $prefix ne "" && $prefix !~ /=$/;
										my $value = undef;
										if (is_key($lookup_key))
										{
												$value = get_value($lookup_key, %{$instance_to_args->{$use_consistent}->{$instance}});
										}
										else
										{
												$value = util::substitute_args($lookup_key, %{$instance_to_args->{$use_consistent}->{$instance}});

										}

										$replacements[0] .= $value;
								}
						}
				}
				elsif ($type eq $raw_keyword)
				{
						my $prefix = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_raw_prefix} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_raw_prefix} : $bind_cache{$cmd_key}{$match_num}{get_cmd_raw_prefix} = get_cmd_raw_prefix(@args);
						my $value = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_raw_value} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_raw_value} : $bind_cache{$cmd_key}{$match_num}{get_cmd_raw_value} = get_cmd_raw_value(@args);
						my $class = $cmd_key && defined $bind_cache{$cmd_key}{$match_num}{get_cmd_raw_class_level} ? $bind_cache{$cmd_key}{$match_num}{get_cmd_raw_class_level} : $bind_cache{$cmd_key}{$match_num}{get_cmd_raw_class_level} = get_cmd_raw_class_level(@args);
						$expanded_class = $class;

						my $class_instances = filter_instances($class_to_instances, $instance_to_args, $class, $cmd_class_level, $query_mode, @args);
						
						if ($query_mode)
						{
								my $cur_good_query = $class_instances->[0];
								$class_instances = $class_instances->[1];
								$good_query = 0 unless $cur_good_query;

								foreach my $cur_class (keys %{$class_instances})
								{
										foreach my $instance (@{$class_instances->{$cur_class}})
										{
												$query_result->{$cur_class}->{$instance} = 1;
										}
								}
								my @value_args = ();
								@value_args = util::get_args($value);
								foreach my $value_arg (@value_args)
								{
										$query_result->{$class}->{$value_arg} = 1;														
								}
						}
						else
						{

								die "No instances for $class in $match" unless $allow_empty || ($class_instances && @{$class_instances});

								foreach my $instance (@{$class_instances})
								{
										die "No args for $instance" unless $instance_to_args->{$use_consistent}->{$instance};

										my @a = values %{$instance_to_args->{$use_consistent}->{$instance}};
										$replacements[0] .= $sep if $replacements[0] ne "";
										$replacements[0] .= $prefix;
										$replacements[0] .= $sep if $prefix ne "" && $prefix !~ /=$/;

										my $next_value = util::substitute_args($value, %{$instance_to_args->{$use_consistent}->{$instance}});

										if ($is_uc)
										{
												$next_value = uc $next_value;
										}
										#in case it had = sign (the delimiter for mod args)
										#when we parse mod args we do not remove escape from = sign
										$next_value =~ s/\\=/=/g;
										$replacements[0] .= $next_value;
								}
						}
				}
				else
				{
						die "Bad include type $type in $match in $cmd";
				}
				next if $query_mode;

				my @new_cmd_texts = ();
				foreach my $cmd_text (@cmd_texts)
				{
						foreach my $replacement (@replacements)
						{
								my $new_text = $cmd_text;
								if ($expanded_class)
								{
										$expanded_something{$expanded_class} = 1 if $replacement;
								}
								$new_text =~ s/$to_replace/$replacement/g;
								push @new_cmd_texts, $new_text;
						}
				}
				@cmd_texts = @new_cmd_texts;
		}


		if ($query_mode)
		{
				return [$good_query, $query_result];
		}
		if ($multiline_for_list_props)
		{
				my $expanded_multiline = $multiline_for_list_props;

				while (defined $expanded_multiline)
				{
						if ($expanded_something{$expanded_multiline})
						{
								last;
						}
						if (has_parent_key($expanded_multiline))
						{
								$expanded_multiline = get_parent_key($expanded_multiline);
						}
						else
						{
								$expanded_multiline = undef;
						}
				}
				unless (defined $expanded_multiline)
				{
						return "";
				}
		}
		my $return_value = (scalar @cmd_texts != 1) ? join("\n", @cmd_texts) : $cmd_texts[0];		
		return $return_value;
}

sub get_meta_table_string($$$$$);

sub get_meta_table_string($$$$$)
{
		my $key = shift;
		#hash with key class, value array of instances
		my $class_to_instances = shift;
		#hash with key instance, value hash of args 
		my $instance_to_args = shift;
		#function to convert an instance to an object
		my $instance_to_object = shift;
		#function to convert instance into a hash, keyed by class, value instance
		my $instance_to_ancestors = shift;

		my $query_mode = shift;

		my $text = "";
		if (has_add_header($key))
		{
				foreach my $cur_text (get_add_header($key))
				{
						$text .= "$cur_text\n";
				}
		}

		my $cmd_class_level = get_class_level($key);
		my $query_result = {};
		eval
		{
				my $have_instances = defined $class_to_instances;
				
				#first, make sure that there is only one leaf class (relative to the classes in this cmd)
				# and that all others are its ancestors
				my %leaf_classes = ();
				my $use_consistent = 1;
				if (defined $class_to_instances->{0})
				{
						$use_consistent = 0;
				}
				map {$leaf_classes{$_} = 1} keys %{$class_to_instances->{$use_consistent}};

				foreach my $class (keys %{$class_to_instances->{$use_consistent}})
				{
						my $cur_class = $class;

						while (has_parent_key($cur_class))
						{
								my $parent_key = get_parent_key($cur_class);
								if ($leaf_classes{$parent_key})
								{
										delete $leaf_classes{$parent_key};
								}
								$cur_class = $parent_key;
						}
				}

				if (scalar keys %leaf_classes > 1)
				{
						if (scalar keys %leaf_classes == 2 && exists $leaf_classes{$cmd_class_level})
						{
								delete $leaf_classes{$cmd_class_level};
						}
						else
						{
								die "More than one leaf class in $key: " . join(",", keys %leaf_classes) . "\n";
						}
				}
				
				my @leaf_classes = keys %leaf_classes;
				my $leaf_class = $leaf_classes[0];

				my $good_query = 1;
				my $needed_props = {};

				if ($query_mode)
				{
						#handle case where querying for needed args
						$query_result = bind_instances_to_cmd($key, $class_to_instances, $instance_to_args, $instance_to_object, $query_mode, $leaf_class);
						$good_query = $query_result->[0];
						$needed_props = $query_result->[1];
				}
				if ($have_instances)
				{
						#validate that we have only one output, and that it is hidden
						my %includes = get_cmd_includes($key);
						my $num_output = 0;
						foreach my $match (keys %includes)
						{
								my @args = @{$includes{$match}->[0]};
								my $type = get_cmd_type(@args);
								if ($type eq $output_keyword)
								{
										my $file_key = get_cmd_input_or_output_file_key(@args);
										my $class_level = get_class_level($file_key);
										if ($class_level ne $cmd_class_level)
										{
												die "Class level of $file_key must match class level of $key\n";
										}
										$num_output++;
										die "Can only have one output for meta table cmds" if $num_output > 1;
										unless (is_hidden_cmd_input_or_output(@args))
										{
												die "All outputs for meta table cmds must be hidden";
										}
								}
						}

						#now, write out the rows

						foreach my $leaf_instance (@{$class_to_instances->{$use_consistent}->{$leaf_class}})
						{
								my %class_to_ancestor_instances = ();

								if (!$use_consistent && $class_to_instances->{1}->{$cmd_class_level})
								{
										$class_to_ancestor_instances{1}->{$cmd_class_level} = $class_to_instances->{1}->{$cmd_class_level};
								}

								%{$class_to_ancestor_instances{$use_consistent}} = &$instance_to_ancestors($leaf_instance);
								foreach my $class (keys %{$class_to_ancestor_instances{$use_consistent}})
								{

										$class_to_ancestor_instances{$use_consistent}->{$class} = [$class_to_ancestor_instances{$use_consistent}->{$class}];

								}

								#add extra classes not directly ancestors of this leaf
								foreach my $cur_class (keys %{$class_to_instances->{$use_consistent}})
								{
										next if $class_to_ancestor_instances{$use_consistent}->{$cur_class};

										my $cur_instances = $class_to_instances->{$use_consistent}->{$cur_class};
										if ($cur_instances)
										{
												$class_to_ancestor_instances{$use_consistent}->{$cur_class} = $cur_instances;
										}
								}

								#see if everything filtered out
								eval
								{
										my $row = bind_instances_to_cmd($key, \%class_to_ancestor_instances, $instance_to_args, $instance_to_object, $query_mode, $leaf_class);
										if ($query_mode)
										{
												my $cur_good_query = $row->[0];
												$good_query = 0 unless $cur_good_query;
												my $cur_result = $row->[1];
												foreach my $class (keys %{$cur_result})
												{
														foreach my $arg (keys %{$cur_result->{$class}})
														{
																$needed_props->{$class}->{$arg} = 1;
														}
												}
										}
										else
										{
												if ($row)
												{
														$text .= "$row\n";
												}
										}
								};
								if ($@ && $@ !~ /No instances for/)
								{
										die $@;
								}
						}
						$query_result = [$good_query, $needed_props];
				}
		};
		if ($@)
		{
				if ($@ eq main::trap_signal())
				{
						die $@;
				}

				die "Error building meta string for $key: $@";
		}
		$text =~ s/\\t/\t/g;
		$text =~ s/\\n/\n/g;

		if ($query_mode)
		{
				return $query_result;
		}
		else
		{
				return $text;
		}
}

#END IDEALLY THIS WOULD BE MOVED INTO A MODULE


sub is_file($)
{
		my $key = shift;

		return is_prefix($key, $file_keyword);
}

sub get_all_files()
{
		my @keys = get_all_keys();
		my @files = ();
		foreach my $key (@keys)
		{
				if (is_file($key))
				{
						push @files, $key;
				}
		}
		return @files;
}

sub is_path($)
{
		my $key = shift;

		return is_prefix($key, $path_keyword);
}

sub missing_okay($)
{
		my $key = shift;

		return is_prefix($key, $missing_okay_keyword);
}

sub optional_input($)
{
		my $key = shift;

		return is_prefix($key, $optional_input_keyword);
}

sub optional_output($)
{
		my $key = shift;

		return is_prefix($key, $optional_output_keyword);
}

sub is_table($)
{
		my $key = shift;

		return is_prefix($key, $table_keyword);
}

sub ensure_header($)
{
		my $key = shift;
		if (!has_header($key))
		{
				die "Need header for $key\n";
		}
}

sub has_header($)
{
		my $key = shift;

		if (!is_table($key))
		{
				die "Error: Can't check whether non table $key has headers";
		}
		return !is_prefix($key, $nohead_keyword);
}

sub has_double_comment($)
{
		my $key = shift;

		return is_prefix($key, $doubcom_keyword);
}


sub is_private($)
{
		my $key = shift;

		return is_prefix($key, $private_keyword);
}

sub has_include_failed($)
{
		my $key = shift;

		if (is_prefix($key, $include_failed_keyword))
		{
				return 1;
		}
		else
		{
				return undef;
		}
}

sub get_restart_long($)
{
		my $key = shift;
		return is_prefix($key, $restart_long_keyword);
}

sub get_restart_mem($)
{
		my $key = shift;
		return is_prefix($key, $restart_mem_keyword);
}

sub get_ignore_md5($)
{
		my $key = shift;
		return is_prefix($key, $ignore_md5_keyword);
}

#postfixes

sub has_postfix_key($$)
{
		my $key = shift;
		my $keyword = shift;

		return exists $postfix_data{$keyword}{$key};
}



sub get_postfix_key($$)
{
		my $key = shift;
		my $keyword = shift;
		if (!has_postfix_key($key, $keyword))
		{
				die "Error: Key $key has no $keyword attribute; can't fetch $keyword key";

		}
		return $postfix_data{$keyword}{$key};
}


sub get_xml_key($)
{
		my $key = shift;

		unless (is_bam($key))
		{
				die "Error: Key $key is not specified to be a bam file; can't fetch xml file";
		}
		return get_postfix_key($key, $xml_keyword);

}

sub has_chmod_value($)
{
		my $key = shift;

		return has_postfix_key($key, $chmod_keyword);
}

sub get_chmod_value($)
{
		my $key = shift;
		if (!has_chmod_value($key))
		{
				die "Error: key $key has no chmod value";
		}

		my $chmod = get_postfix_key($key, $chmod_keyword);

		unless ($chmod =~ /[0-7][0-7][0-7]/)
		{
				die "Error: chmod $chmod for key $key is not of the correct format";
		}
		return $chmod;
}

sub has_skip_re_value($)
{
		my $key = shift;

		return has_postfix_key($key, $skip_re_keyword);
}

sub get_skip_re_value($)
{
		my $key = shift;
		if (!has_skip_re_value($key))
		{
				die "Error: key $key has no skip_re value";
		}

		my $skip_re = get_postfix_key($key, $skip_re_keyword);

		return $skip_re;
}

sub has_class_level($)
{
		my $key = shift;

		return has_postfix_key($key, $class_level_keyword);
}

sub get_class_level($)
{
		my $key = shift;
		if (!has_class_level($key))
		{
				die "Error: key $key (" . get_value($key, 1) . ") has no class level value";
		}

		my $class_level = get_postfix_key($key, $class_level_keyword);

		if (!is_class($class_level))
		{
				die "Getting class level for $key: $class_level is not a class\n";
		}

		return $class_level;
}

sub has_timeout($)
{
		my $key = shift;

		return has_postfix_key($key, $timeout_keyword);
}

sub get_timeout($)
{
		my $key = shift;
		if (!has_timeout($key))
		{
				die "Error: key $key has no timeout value";
		}

		my $timeout = get_postfix_key($key, $timeout_keyword);

		return $timeout;
}

sub has_default($)
{
		my $key = shift;

		return has_postfix_key($key, $default_keyword);
}

sub get_default($)
{
		my $key = shift;
		if (!has_default($key))
		{
				die "Error: key $key has no default value";
		}
		my $default = get_postfix_key($key, $default_keyword);

		return $default;
}

sub has_add_header($)
{
		my $key = shift;

		return has_postfix_key($key, $add_header_keyword);
}

sub get_add_header($)
{
		my $key = shift;
		if (!has_add_header($key))
		{
				die "Error: key $key has no add header value";
		}

		my $add_header_ref = get_postfix_key($key, $add_header_keyword);
		return @{$add_header_ref};
}

sub has_key_col_value($)
{
		my $key = shift;

		return has_postfix_key($key, $key_col_keyword);
}


sub get_key_col_value($)
{
		my $key = shift;
		if (!has_key_col_value($key))
		{
				die "Error: key $key has no key_col value";
		}

		my $array_ref = get_postfix_key($key, $key_col_keyword);
		my @key_cols = @{$array_ref};

		foreach (my $i = 0; $i <= $#key_cols; $i++)
		{
				my @tmp = split("$vector_arg_delim", $key_cols[$i]);
				for (my $i = $#tmp; $i > 0; $i--)
				{
						if ($tmp[$i - 1] =~ /^(.*)\\$/)
						{
								$tmp[$i - 1] = "$1$vector_arg_delim$tmp[$i]";
						}
				}
				$key_cols[$i] = \@tmp;
		}

		return @key_cols;
}

sub has_tiebreak_col_value($)
{
		my $key = shift;

		return has_postfix_key($key, $tiebreak_col_keyword);
}


sub get_tiebreak_col_value($)
{
		my $key = shift;
		if (!has_tiebreak_col_value($key))
		{
				die "Error: key $key has no tiebreak_col value";
		}

		my $tiebreak_col = get_postfix_key($key, $tiebreak_col_keyword);

		return $tiebreak_col;
}

sub has_env_mod_value($)
{
		my $key = shift;

		return has_postfix_key($key, $env_mod_keyword);
}


sub get_env_mod_values($)
{
		my $key = shift;
		if (!has_env_mod_value($key))
		{
				die "Error: key $key has no env_mod value";
		}

		my @env_mods = @{&get_postfix_key($key, $env_mod_keyword)};

		foreach (my $i = 0; $i <= $#env_mods; $i++)
		{
				my @tmp = split($vector_arg_delim, $env_mods[$i]);
				die "Each element of env_mod must be of the form VAR${vector_arg_delim}VALUE: failed on $env_mods[$i]\n" unless scalar @tmp >= 2;

				foreach (my $j = 1; $j <= $#tmp; $j++)
				{
						if ($tmp[$j] =~ /^\$(.+)$/)
						{
								$tmp[$j] = get_value($1)
						}
				}
				$env_mods[$i] = \@tmp;
		}

		return @env_mods;
}

sub parse_if_value_clauses($)
{
		#return: a list with 4 elements:
		#1. whether the whole expression should be complemented
		#2. whether the conjunction is AND
		#3. a list of the terms
    #4. a list of whether each term is a clause or a variable

		my $value = shift;
		my $complement = 0;
		my $conjunction = 1;
		my @clauses = ();
		my @is_clause = ();

		if ($value =~ /^!(.+)$/ || $value =~ /^\(!(.+)\)$/)
		{
				$complement = 1;
				$value = $1;
		}
		if ($value =~ /^\((.+)\)$/)
		{
				$value = $1;
		}

		my $paren_nest = 0;
		my $cur_clause = "";
		for (my $i = 0; $i < length($value); $i++)
		{
				my $cur_char = substr($value, $i, 1);
				if ($paren_nest < 0)
				{
						die "Error: bad parentheses in expression $value\n";
				}
				if ($cur_char eq "," && $paren_nest == 0)
				{
						push @clauses, $cur_clause;
						$cur_clause = "";
				}
				else
				{
						$cur_clause .= $cur_char;
						if ($cur_char eq "(")
						{
								$paren_nest++;
						}
						elsif ($cur_char eq ")")
						{
								$paren_nest--;
						}
				}
		}
		push @clauses, $cur_clause;
		
		if (scalar @clauses > 1)
		{
				if ($clauses[0] eq $and_conjunction)
				{
						$conjunction = 1;
				}
				elsif ($clauses[0] eq $or_conjunction)
				{
						$conjunction = 0;
				}
				else
				{
						die "For multiple run_if or skip_if values: first must be $and_conjunction or $or_conjunction: $value\n";
				}
				@clauses = @clauses[1..$#clauses];
		}
		foreach my $clause (@clauses)
		{
				if ($clause =~ /^\(.*\)/ || $clause =~ /^!/)
				{
						push @is_clause, 1;
				}
				else
				{
						push @is_clause, 0;
				}
		}

		return [$complement, $conjunction, \@clauses, \@is_clause];
}

sub has_skip_if_value($)
{
		my $key = shift;

		return has_postfix_key($key, $skip_if_keyword);
}

sub get_skip_if_value($)
{
		my $key = shift;
		if (!has_skip_if_value($key))
		{
				die "Error: key $key has no skip_if value";
		}

		my $skip_if_value = get_postfix_key($key, $skip_if_keyword);
		return $skip_if_value;
}


sub has_run_if_value($)
{
		my $key = shift;

		return has_postfix_key($key, $run_if_keyword);
}


sub get_run_if_value($)
{
		my $key = shift;
		if (!has_run_if_value($key))
		{
				die "Error: key $key has no run_if value";
		}

		my $run_if_value = get_postfix_key($key, $run_if_keyword);
		return $run_if_value;
}

sub has_umask_mod_value($)
{
		my $key = shift;

		return has_postfix_key($key, $umask_mod_keyword);
}

sub get_umask_mod_value($)
{
		my $key = shift;
		if (!has_umask_mod_value($key))
		{
				die "Error: key $key has no umask_mod value";
		}

		my $umask_mod = get_postfix_key($key, $umask_mod_keyword);

		return $umask_mod;
}

sub has_rusage_mod_value($)
{
		my $key = shift;

		return has_postfix_key($key, $rusage_mod_keyword);
}

sub get_rusage_mod_value($)
{
		my $key = shift;
		if (!has_rusage_mod_value($key))
		{
				die "Error: key $key has no rusage_mod value";
		}

		my $rusage_mod = get_postfix_key($key, $rusage_mod_keyword);

		return $rusage_mod;
}

sub has_update_ext_value($)
{
		my $key = shift;

		return has_postfix_key($key, $update_ext_keyword);
}

sub get_update_ext_value($)
{
		my $key = shift;
		if (!has_update_ext_value($key))
		{
				die "Error: key $key has no update_ext value";
		}

		my $update_ext = get_postfix_key($key, $update_ext_keyword);

		return $update_ext;
}

sub has_bsub_batch_value($)
{
		my $key = shift;
		return has_postfix_key($key, $bsub_batch_keyword);
}

sub get_bsub_batch_value($)
{
		my $key = shift;
		if (!has_bsub_batch_value($key))
		{
				die "Error: key $key has no bsub_batch value";
		}

		my $bsub_batch = get_postfix_key($key, $bsub_batch_keyword);

		return $bsub_batch;
}


sub has_bai_key($)
{
		my $key = shift;

		return has_postfix_key($key, $bai_keyword);

}

sub get_bai_key($)
{
		my $key = shift;

		unless (has_bai_key($key))
		{
				die "Error: Key $key has no bai key";
		}
		return get_postfix_key($key, $bai_keyword);

}

sub has_parent_key($)
{
		my $key = shift;

		return has_postfix_key($key, $parent_keyword);

}

sub get_parent_key($)
{
		my $key = shift;

		unless (has_parent_key($key))
		{
				die "Error: Key $key has no parent key";
		}
		my $parent = get_postfix_key($key, $parent_keyword);
		die "Can't set parent of $key equal to itself" if $key eq $parent;
		return $parent;
}

sub get_ancestor_keys($)
{
		my $key = shift;
		return () unless $key;
		my %ancestors = ($key=>1);
		my %seen_keys = ();
		while (has_parent_key($key))
		{
				die "Error: Circular parent relationship in config file" if $seen_keys{$key};
				$seen_keys{$key} = 1;
				$key = get_parent_key($key);
				$ancestors{$key} = 1;
		}
		return %ancestors;
}

sub has_consistent_key($)
{
		my $key = shift;

		return has_postfix_key($key, $consistent_keyword);

}
sub get_consistent_key($)
{
		my $key = shift;

		unless (has_consistent_key($key))
		{
				die "Error: Key $key has no consistent key";
		}
		my $consistent = get_postfix_key($key, $consistent_keyword);
		die "Can't set consistent of $key equal to itself" if $key eq $consistent;
		return @{$consistent};
}

sub has_consistent_prop_key($)
{
		my $key = shift;

		return has_postfix_key($key, $consistent_prop_keyword);

}
sub get_consistent_prop_key($)
{
		my $key = shift;

		unless (has_consistent_prop_key($key))
		{
				die "Error: Key $key has no consistent_prop key";
		}
		my $consistent_prop = get_postfix_key($key, $consistent_prop_keyword);
		return $consistent_prop;
}


sub has_display_value($)
{
		my $key = shift;

		return has_postfix_key($key, $disp_keyword);

}

sub get_display_value($)
{
		my $key = shift;

		unless (has_display_value($key))
		{
				die "Error: Key $key has no disp value";
		}
		return get_postfix_key($key, $disp_keyword);
}

sub get_bam_summary_key($)
{
		my $key = shift;

		unless (is_bam($key))
		{
				die "Error: Key $key is not specified to be a bam file; can't fetch summary file";
		}

		return get_postfix_key($key, $summary_keyword);
}

sub has_dir_key($)
{
		my $key = shift;

		return has_postfix_key($key, $dir_keyword);
}

sub get_dir_key($@)
{
		my $key = shift;

		return get_postfix_key($key, $dir_keyword);
}

sub has_dependencies($)
{
		my $key = shift;
		return has_postfix_key($key, $depends_keyword);
}

sub get_dependencies($)
{
		my $key = shift;

		my $depends_ref = get_postfix_key($key, $depends_keyword);
		return @{$depends_ref};
}

sub has_with($)
{
		my $key = shift;
		return has_postfix_key($key, $with_keyword);
}

sub get_with($)
{
		my $key = shift;

		my $with_ref = get_postfix_key($key, $with_keyword);
		return @{$with_ref};
}

sub has_run_with($)
{
		my $key = shift;
		return has_postfix_key($key, $run_with_keyword);
}

sub get_run_with($)
{
		my $key = shift;

		my $with_ref = get_postfix_key($key, $run_with_keyword);
		return @{$with_ref};
}

sub get_meta_levels($)
{
		my $key = shift;

		my $meta_levels_ref = get_postfix_key($key, $meta_level_keyword);
		my @meta_levels = @{$meta_levels_ref};
		foreach my $meta_level (@meta_levels)
		{
				if (!is_class($meta_level))
				{
						die "Bad value for $meta_level_keyword: $meta_level is not a class\n";
				}
		}
		return @meta_levels;

}

my $that_depend_cache = undef;
sub get_those_that_depend($)
{
		my $key = shift;
		unless (defined $that_depend_cache)
		{
				$that_depend_cache = {};
				foreach my $cur_key (get_all_keys())
				{
						if (has_dependencies($cur_key))
						{
								foreach my $dependency (get_dependencies($cur_key))
								{
										push @{$that_depend_cache->{$dependency}}, $cur_key;
								}
						}
				}

		}
		if ($that_depend_cache->{$key})
		{
				return @{$that_depend_cache->{$key}};
		}
		else
		{
				return ();
		}
}

sub get_args($@);

sub get_args($@)
{
		my $key = shift;
		my $add_dir = shift;

		if (is_private($key))
		{
				die "Error: Can't access private property $key\n";
		}

		unless (is_key($key))
		{
				die "Error: No key $key\n"; 
		}

		my $value = get_expanded_value($key);
		my @to_return = util::get_args($value);

		if ($add_dir)
		{
				push @to_return, get_args(get_dir_key($key));
		}

		return @to_return;

}

sub get_path($@)
{
		my $key = shift;
		my $file = get_value($key, @_);
		my $dir = get_dir($key, @_);
		return util::cat_dir_file($dir, $file);
}

1;
