fn circuit() -> impl FnMut(OrdZSet<Tuple2<i32, F64>, Weight>) -> (OrdZSet<Tuple1<i32>, Weight>, ) {
    let T = Rc::new(RefCell::<OrdZSet<Tuple2<i32, F64>, Weight>>::new(Default::default()));
    let T_external = T.clone();
    let T = Generator::new(move || T.borrow().clone());
    let V = Rc::new(RefCell::<OrdZSet<Tuple1<i32>, Weight>>::new(Default::default()));
    let V_external = V.clone();
    let root = Circuit::build(|circuit| {
        let map6952: _ = move |t: &Tuple2<i32, F64>, | -> Tuple1<i32> {
            Tuple1::new(t.0)
        };
        let T = circuit.add_source(T);
        let stream6956: Stream<_, OrdZSet<Tuple1<i32>, Weight>> = T.map(map6952);
        // CREATE VIEW `V` AS
        // SELECT `COL1`
        // FROM `T`
        stream6956.inspect(move |m| { *V.borrow_mut() = m.clone() });
    }).unwrap();
    return move |T| {
        *T_external.borrow_mut() = T;
        root.0.step().unwrap();
        return (V_external.borrow().clone(), );
    };
}
