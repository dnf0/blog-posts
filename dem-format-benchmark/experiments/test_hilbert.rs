fn hilbert_encode(mut x: u32, mut y: u32) -> u64 {
    let mut d: u64 = 0;
    
    for i in (0..32).rev() {
        let rx = (x >> i) & 1;
        let ry = (y >> i) & 1;
        
        let quad = (3 * rx) ^ ry;
        d += (quad as u64) << (2 * i);
        
        if ry == 0 {
            if rx == 1 {
                let mask = (1u32 << i).wrapping_sub(1);
                x = x ^ mask;
                y = y ^ mask;
            }
            let t = x;
            x = y;
            y = t;
        }
    }
    d
}

fn main() {
    println!("(0,0) -> {}", hilbert_encode(0, 0));
    println!("(1,0) -> {}", hilbert_encode(1, 0));
    println!("(1,1) -> {}", hilbert_encode(1, 1));
    println!("(0,1) -> {}", hilbert_encode(0, 1));
    println!("(2,0) -> {}", hilbert_encode(2, 0));
    println!("(3,0) -> {}", hilbert_encode(3, 0));
    println!("MAX -> {}", hilbert_encode(0xFFFFFFFF, 0xFFFFFFFF));
}
