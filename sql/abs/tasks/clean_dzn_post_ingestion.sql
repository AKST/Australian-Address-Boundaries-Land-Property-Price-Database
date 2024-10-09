--
-- For whatever reason when you initially load abs.dzn
-- it there are some rows with sa2_code's like this:
--
--    - 0@@@@@@@
--    - 0&&&&&&&
--    - 0VVVVVVV
--
-- It's not that exactly but if you use the where
-- clause below you'll find that data. Their respective
-- state codes are somethign like `@`, `&` and `V`. I
-- am not sure what's going on here...
--
UPDATE abs.dzn as dzn
   SET sa2_code = NULL,
       state_code = NULL
 WHERE NOT EXISTS (SELECT 1 FROM abs.sa2 as sa2
                    WHERE dzn.sa2_code = sa2.sa2_code);
