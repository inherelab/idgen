package server

const (
	CreateSingleTableSQL = `
CREATE TABLE %s (
	id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
    sys VARCHAR(128) NOT NULL COMMENT "system name",
    PRIMARY KEY (id)
) ENGINE=Innodb DEFAULT CHARSET=utf8`

)