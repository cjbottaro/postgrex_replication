defmodule PgCdcTest do
  use ExUnit.Case
  doctest PgCdc

  @sample {
    "BEGIN 3772",
    "table public.users: UPDATE: old-key: id[integer]:1 email[text]:'superuser@academicworks.com' encrypted_password[text]:'' sign_in_count[integer]:0 confirmed_at[timestamp without time zone]:'2016-12-10 02:21:53.024143' failed_attempts[integer]:0 created_at[timestamp without time zone]:'2016-12-10 02:21:53.028182' updated_at[timestamp without time zone]:'2016-12-20 19:59:39.708035' display_name[text]:'super7' authorized[boolean]:true enable_fullscreen_reviews[boolean]:false applicant_filters[jsonb]:'{}' landing_page[character varying]:'opportunities_dashboard' new-tuple: id[integer]:1 email[text]:'superuser@academicworks.com' encrypted_password[text]:'' reset_password_token[text]:null sign_in_count[integer]:0 current_sign_in_at[timestamp without time zone]:null last_sign_in_at[timestamp without time zone]:null current_sign_in_ip[text]:null last_sign_in_ip[text]:null confirmation_token[text]:null confirmed_at[timestamp without time zone]:'2016-12-10 02:21:53.024143' confirmation_sent_at[timestamp without time zone]:null failed_attempts[integer]:0 unlock_token[text]:null locked_at[timestamp without time zone]:null created_at[timestamp without time zone]:'2016-12-10 02:21:53.028182' updated_at[timestamp without time zone]:'2016-12-20 20:00:55.0039' invitation_token[character varying]:null invitation_sent_at[timestamp without time zone]:null customer_uid[text]:null display_name[text]:'super8'accepted_at[timestamp without time zone]:null authorized[boolean]:true invitation_limit[integer]:null invited_by_id[integer]:null invited_by_type[character varying]:null reset_password_sent_at[timestamp without time zone]:null enable_fullscreen_reviews[boolean]:false previous_passwords[text]:null dashboard_preferences[text]:null invitation_created_at[timestamp without time zone]:null avatar_id[integer]:null applicant_filters[jsonb]:'{}' landing_page[character varying]:'opportunities_dashboard'",
    "COMMIT 3772"
  }

  test "parse change" do
    change = @sample |> elem(1) |> PgCdc.Change.parse
    assert change.table == "public.users"
    assert change.type == "UPDATE"
    assert change.changes["display_name"] == {"super7", "super8"}
    assert Map.has_key?(change.changes, "updated_at")
  end

end
