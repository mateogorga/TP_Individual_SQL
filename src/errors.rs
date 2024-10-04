pub enum ErrorType {
    InvalidTable,
    InvalidColumn,
    InvalidSintax,
    Error,
}

pub fn mostrar_error(tipo: ErrorType, descripcion: &str) {

    match tipo {
        ErrorType::InvalidColumn => println!("[INVALID_COLUMN]: {}", descripcion),
        ErrorType::InvalidTable => println!("[INVALID_TABLE]: {}", descripcion),
        ErrorType::InvalidSintax => println!("[INVALID_SINTAX]: {}", descripcion),
        ErrorType::Error => println!("[ERROR]: {}", descripcion),
    }
}