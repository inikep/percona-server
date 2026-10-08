# $rx_result must be the text returned by set_filter or set_user.
if ($rx_result != OK)
{
  --enable_result_log
  --echo $rx_result
  --die audit filter change failed
}
