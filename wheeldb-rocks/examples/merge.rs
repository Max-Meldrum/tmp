use hierarchical_aggregation_wheel::time::*;
use hierarchical_aggregation_wheel::Entry;
use wheeldb_rocks::db::WheelDB;

fn main() {
    let mut db = WheelDB::open_default_with_dimensions("/tmp/wheeldb_rocks", &["country"]);
    db.merge("hej", Entry::new(1.0, 500));
    db.merge("hej", Entry::new(1.0, 1600000));
    db.merge("hej", Entry::new(1.0, 1000));
    db.merge("hej", Entry::new(1.0, 3000));
    db.merge("hej", Entry::new(1.0, 6000));

    //db.dimension("city").merge("Stockholm")
    db.merge("då", Entry::new(1.0, 1000));
    db.merge("då", Entry::new(1.0, 3000));
    db.merge("då", Entry::new(1.0, 6000));

    db.dimension("country")
        .merge("Sweden", Entry::new(100.0, 3000));

    db.advance_to(14000);

    let sweden_wheel = db.dimension("country").get("Sweden").unwrap();
    println!("sweden range full {:?}", sweden_wheel.range(..));

    let wheel = db.get("hej").unwrap();
    println!("Interval last 2 seconds {:?}", wheel.interval(2.seconds()));
    println!(
        "Interval last 10 seconds {:?}",
        wheel.interval(10.seconds())
    );
    println!("range full {:?}", wheel.range(..));
    //println!("range full {:?}", wheel.range(datetime!()..wheel.now()));
    println!("now {:?}", wheel.now());

    let star_wheel = db.get_star_wheel_mut();
    star_wheel.advance(10.seconds());
    println!(
        "Interval last 10 seconds {:?}",
        star_wheel.interval(10.seconds())
    );
    println!("range full {:?}", star_wheel.range(..));
}
