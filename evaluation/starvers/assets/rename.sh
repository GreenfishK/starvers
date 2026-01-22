find . -depth -print0 | xargs -0 rename \
  's/icng/snapshots_ng/g;
   s/ictr/plain/g;
   s/tb_sr_ng/bear_ng/g;
   s/tb_sr_rs/starvers/g'


find . -type f -print0 | xargs -0 sed -i \
  -e 's/icng/snapshots_ng/g;
  -e s/ictr/plain/g;
  -e s/tb_sr_ng/bear_ng/g;
  -e s/tb_sr_rs/starvers/g'



find . -type f -print0 | xargs -0 sed -i \
  -e 's/ic_sr_ng/snapshots/g' \
  -e 's/cb_sr_ng/deltas/g' \
  -e 's/tb_sr_ng/bear/g' \
  -e 's/tb_sr_rs/starvers/g'


find . -depth -print0 | xargs -0 rename \
  's/ic_sr_ng/snapshots/g;
   s/cb_sr_ng/deltas/g;
   s/tb_sr_ng/bear/g;
   s/tb_sr_rs/starvers/g'