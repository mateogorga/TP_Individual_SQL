use crate::dividir_consulta;

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::{BufWriter, Write};
    
    // Prueba para la función dividir_consulta
    #[test]
    fn test_dividir_consulta() {
        let consulta = "INSERT INTO clientes (id, nombre, apellido) VALUES (20, 'Lucas', 'Perez'), 
    (21, 'Juan', 'Benitez'), (22, 'Maria', 'Fernandez');".to_string();
        let resultado = dividir_consulta(&consulta);
        assert_eq!(resultado, vec!["INSERT", "INTO", "clientes", "(id, nombre, apellido)", "VALUES", "(20, 'Lucas', 'Perez')", ",", "(21, 'Juan', 'Benitez')", ",", "(22, 'Maria', 'Fernandez')"]);
    }
}