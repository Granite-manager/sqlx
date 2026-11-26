use super::MySqlStream;
use crate::connection::stream::Waiting;
use crate::error::Error;
use crate::executor::{Execute, Executor};
use crate::ext::ustr::UStr;
use crate::io::MySqlBufExt;
use crate::logger::QueryLogger;
use crate::protocol::response::Status;
use crate::protocol::statement::{
    BinaryRow, Execute as StatementExecute, Prepare, PrepareOk, StmtClose,
};
use crate::protocol::text::{ColumnDefinition, Query, TextRow};
use crate::statement::{MySqlStatement, MySqlStatementMetadata};
use crate::HashMap;
use crate::{
    MySql, MySqlArguments, MySqlColumn, MySqlConnection, MySqlQueryResult, MySqlRow, MySqlTypeInfo,
    MySqlValueFormat,
};
use either::Either;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_core::Stream;
use futures_util::TryStreamExt;
use sqlx_core::column::{ColumnOrigin, TableColumn};
use sqlx_core::sql_str::SqlStr;
use std::{pin::pin, sync::Arc};

impl MySqlConnection {
    async fn prepare_statement(
        &mut self,
        sql: &str,
    ) -> Result<(u32, MySqlStatementMetadata), Error> {
        // https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html
        // https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK

        self.inner
            .stream
            .send_packet(Prepare { query: sql })
            .await?;

        let ok: PrepareOk = self.inner.stream.recv().await?;

        // the parameter definitions are very unreliable so we skip over them
        // as we have little use

        if ok.params > 0 {
            for _ in 0..ok.params {
                let _def: ColumnDefinition = self.inner.stream.recv().await?;
            }

            self.inner.stream.maybe_recv_eof().await?;
        }

        // the column definitions are berefit the type information from the
        // to-be-bound parameters; we will receive the output column definitions
        // once more on execute so we wait for that

        let mut columns = Vec::new();

        let column_names = if ok.columns > 0 {
            recv_result_metadata(&mut self.inner.stream, ok.columns as usize, &mut columns).await?
        } else {
            Default::default()
        };

        let id = ok.statement_id;
        let metadata = MySqlStatementMetadata {
            parameters: ok.params as usize,
            columns: Arc::new(columns),
            column_names: Arc::new(column_names),
        };

        Ok((id, metadata))
    }

    async fn get_or_prepare_statement(
        &mut self,
        sql: &str,
    ) -> Result<(u32, MySqlStatementMetadata), Error> {
        if let Some(statement) = self.inner.cache_statement.get_mut(sql) {
            // <MySqlStatementMetadata> is internally reference-counted
            return Ok((*statement).clone());
        }

        let (id, metadata) = self.prepare_statement(sql).await?;

        // in case of the cache being full, close the least recently used statement
        if let Some((id, _)) = self
            .inner
            .cache_statement
            .insert(sql, (id, metadata.clone()))
        {
            self.inner.result_metadata.remove(&id);
            self.inner
                .stream
                .send_packet(StmtClose { statement: id })
                .await?;
        }

        Ok((id, metadata))
    }

    #[allow(clippy::needless_lifetimes)]
    pub(crate) async fn run<'e, 'c: 'e, 'q: 'e>(
        &'c mut self,
        sql: SqlStr,
        arguments: Option<MySqlArguments>,
        persistent: bool,
    ) -> Result<impl Stream<Item = Result<Either<MySqlQueryResult, MySqlRow>, Error>> + 'e, Error>
    {
        let mut logger = QueryLogger::new(sql, self.inner.log_settings.clone());

        self.inner.stream.wait_until_ready().await?;
        self.inner.stream.waiting.push_back(Waiting::Result);
        // granite patch: until the query is sent, `waiting` expects a
        // response nobody asked for (see `MySqlStream::in_command`).
        self.inner.stream.in_command = true;

        Ok(try_stream! {
        let sql = logger.sql().as_str();

            // make a slot for the shared column data
            // as long as a reference to a row is not held past one iteration, this enables us
            // to re-use this memory freely between result sets
            let mut columns = Arc::new(Vec::new());

            // granite patch: the cached statement run, if one is
            let mut cached_statement = None;

            let format = if let Some(arguments) = arguments {
                if persistent && self.inner.cache_statement.is_enabled() {
                    let (id, metadata) = self
                        .get_or_prepare_statement(sql)
                        .await?;
                    cached_statement = Some(id);

                    if arguments.types.len() != metadata.parameters {
                        return Err(
                            err_protocol!(
                                "prepared statement expected {} parameters but {} parameters were provided",
                                metadata.parameters,
                                arguments.types.len()
                            )
                        );
                    }

                    // https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
                    self.inner.stream
                        .send_packet(StatementExecute {
                            statement: id,
                            arguments: &arguments,
                        })
                        .await?;
                    self.inner.stream.in_command = false;

                    MySqlValueFormat::Binary
                } else {
                    let (id, metadata) = self
                        .prepare_statement(sql)
                        .await?;

                    if arguments.types.len() != metadata.parameters {
                        return Err(
                            err_protocol!(
                                "prepared statement expected {} parameters but {} parameters were provided",
                                metadata.parameters,
                                arguments.types.len()
                            )
                        );
                    }

                    // https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
                    self.inner.stream
                        .send_packet(StatementExecute {
                            statement: id,
                            arguments: &arguments,
                        })
                        .await?;
                    self.inner.stream.in_command = false;

                    self.inner.stream.send_packet(StmtClose { statement: id }).await?;

                    MySqlValueFormat::Binary
                }
            } else {
                // https://dev.mysql.com/doc/internals/en/com-query.html
                self.inner.stream.send_packet(Query(sql)).await?;
                self.inner.stream.in_command = false;

                MySqlValueFormat::Text
            };

            loop {
                // query response is a meta-packet which may be one of:
                //  Ok, Err, ResultSet, or (unhandled) LocalInfileRequest
                let mut packet = self.inner.stream.recv_packet().await?;

                if packet[0] == 0x00 || packet[0] == 0xff {
                    // first packet in a query response is OK or ERR
                    // this indicates either a successful query with no rows at all or a failed query
                    let ok = packet.ok()?;

                    self.inner.status_flags = ok.status;

                    let rows_affected = ok.affected_rows;
                    logger.increase_rows_affected(rows_affected);
                    let done = MySqlQueryResult {
                        rows_affected,
                        last_insert_id: ok.last_insert_id,
                    };

                    r#yield!(Either::Left(done));

                    if ok.status.contains(Status::SERVER_MORE_RESULTS_EXISTS) {
                        // more result sets exist, continue to the next one
                        continue;
                    }

                    self.inner.stream.waiting.pop_front();
                    return Ok(());
                }

                // otherwise, this first packet is the start of the result-set metadata,
                *self.inner.stream.waiting.front_mut().unwrap() = Waiting::Row;

                let num_columns = packet.get_uint_lenenc()?; // column count
                let num_columns = usize::try_from(num_columns)
                    .map_err(|_| err_protocol!("column count overflows usize: {num_columns}"))?;

                // Always reload column names, even for prepared statements (the schema
                // may change between PREPARE and EXECUTE).
                // granite patch: a cached statement whose columns arrive as they did
                // last time shares what was decoded then (`recv_result_metadata_reusing`).
                let column_names = match cached_statement {
                    Some(id) => {
                        let inner = &mut *self.inner;
                        let (known_columns, column_names) = recv_result_metadata_reusing(
                            &mut inner.stream,
                            &mut inner.result_metadata,
                            id,
                            num_columns,
                        )
                        .await?;
                        columns = known_columns;
                        column_names
                    }
                    None => Arc::new(recv_result_metadata(&mut self.inner.stream, num_columns, Arc::make_mut(&mut columns)).await?),
                };

                // finally, there will be none or many result-rows
                loop {
                    let packet = self.inner.stream.recv_packet().await?;

                    if packet[0] == 0xfe {
                        let (rows_affected, last_insert_id, status) = if packet.len() < 9 {
                            // EOF packet
                            let eof = packet.eof(self.inner.stream.capabilities)?;
                            (0, 0, eof.status)
                        } else {
                            // OK packet
                            let ok = packet.ok()?;
                            (ok.affected_rows, ok.last_insert_id, ok.status)
                        };

                        self.inner.status_flags = status;
                        r#yield!(Either::Left(MySqlQueryResult {
                            rows_affected,
                            last_insert_id,
                        }));

                        if status.contains(Status::SERVER_MORE_RESULTS_EXISTS) {
                            *self.inner.stream.waiting.front_mut().unwrap() = Waiting::Result;
                            break;
                        }
                        self.inner.stream.waiting.pop_front();
                        return Ok(());
                    }

                    let row = match format {
                        MySqlValueFormat::Binary => packet.decode_with::<BinaryRow, _>(&columns)?.0,
                        MySqlValueFormat::Text => packet.decode_with::<TextRow, _>(&columns)?.0,
                    };

                    let v = Either::Right(MySqlRow {
                        row,
                        format,
                        columns: Arc::clone(&columns),
                        column_names: Arc::clone(&column_names),
                    });

                    logger.increment_rows_returned();

                    r#yield!(v);
                }
            }
        })
    }
}

/// granite patch: the columns a cached statement answered with, kept with
/// the bytes of their definitions as the server sent them.
///
/// The server sends every column's definition again with each execution and
/// upstream decodes them again (a name and an origin allocated per column,
/// a map of names per result). The definitions of a statement run again are
/// almost always the same bytes: then the decoded columns are shared. Any
/// difference (the schema changed) decodes them anew, as upstream always does.
#[derive(Clone)]
pub(crate) struct ResultMetadata {
    definitions: Arc<[Box<[u8]>]>,
    columns: Arc<Vec<MySqlColumn>>,
    column_names: Arc<HashMap<UStr, usize>>,
}

/// granite patch: [`recv_result_metadata`] for the cached statement `id`,
/// sharing its columns when they arrive as they did last time
/// ([`ResultMetadata`]).
async fn recv_result_metadata_reusing(
    stream: &mut MySqlStream,
    known: &mut HashMap<u32, ResultMetadata>,
    id: u32,
    num_columns: usize,
) -> Result<(Arc<Vec<MySqlColumn>>, Arc<HashMap<UStr, usize>>), Error> {
    // the limit `recv_result_metadata` enforces
    if num_columns > 4096 {
        return Err(err_protocol!(
            "result-set metadata claims {num_columns} columns but the maximum supported is 4096"
        ));
    }

    let last = known
        .get(&id)
        .filter(|last| last.definitions.len() == num_columns)
        .cloned();
    let mut same = last.is_some();
    // the definitions received, kept only once one differs
    let mut received: Vec<bytes::Bytes> = Vec::new();

    for ordinal in 0..num_columns {
        let packet = stream.recv_packet().await?.0;

        if same {
            if let Some(last) = &last {
                if last.definitions[ordinal][..] == packet[..] {
                    continue;
                }
                received.reserve(num_columns);
                received.extend(
                    last.definitions[..ordinal]
                        .iter()
                        .map(|definition| bytes::Bytes::copy_from_slice(definition)),
                );
            }
            same = false;
        }

        received.push(packet);
    }

    stream.maybe_recv_eof().await?;

    if let (true, Some(last)) = (same, last) {
        return Ok((last.columns, last.column_names));
    }

    let mut columns = Vec::with_capacity(num_columns);
    let mut column_names = HashMap::with_capacity(num_columns);

    for (ordinal, packet) in received.iter().enumerate() {
        let def = <ColumnDefinition as sqlx_core::io::ProtocolDecode<'_, _>>::decode_with(
            packet.clone(),
            stream.capabilities,
        )?;
        let column = recv_next_result_column(&def, ordinal)?;

        column_names.insert(column.name.clone(), ordinal);
        columns.push(column);
    }

    let metadata = ResultMetadata {
        definitions: received.iter().map(|packet| packet[..].into()).collect(),
        columns: Arc::new(columns),
        column_names: Arc::new(column_names),
    };
    let found = (
        Arc::clone(&metadata.columns),
        Arc::clone(&metadata.column_names),
    );
    known.insert(id, metadata);

    Ok(found)
}

impl MySqlConnection {
    /// granite patch: [`Self::run`] for a caller that wants every row at
    /// once: the same commands and packets in the same order, the rows
    /// pushed onto a `Vec` here instead of handed up one at a time through
    /// two streams (`run`'s and `fetch_many`'s) and the caller's collect.
    async fn run_all(
        &mut self,
        sql: SqlStr,
        arguments: Option<MySqlArguments>,
        persistent: bool,
    ) -> Result<Vec<MySqlRow>, Error> {
        let mut logger = QueryLogger::new(sql, self.inner.log_settings.clone());

        self.inner.stream.wait_until_ready().await?;
        self.inner.stream.waiting.push_back(Waiting::Result);
        // granite patch: until the query is sent, `waiting` expects a
        // response nobody asked for (see `MySqlStream::in_command`).
        self.inner.stream.in_command = true;

        let mut columns = Arc::new(Vec::new());
        let mut cached_statement = None;
        let mut rows = Vec::new();

        let format = {
            let sql = logger.sql().as_str();

            if let Some(arguments) = arguments {
                let cached = persistent && self.inner.cache_statement.is_enabled();
                let (id, metadata) = if cached {
                    self.get_or_prepare_statement(sql).await?
                } else {
                    self.prepare_statement(sql).await?
                };

                if arguments.types.len() != metadata.parameters {
                    return Err(err_protocol!(
                        "prepared statement expected {} parameters but {} parameters were provided",
                        metadata.parameters,
                        arguments.types.len()
                    ));
                }

                self.inner
                    .stream
                    .send_packet(StatementExecute {
                        statement: id,
                        arguments: &arguments,
                    })
                    .await?;
                self.inner.stream.in_command = false;

                if cached {
                    cached_statement = Some(id);
                } else {
                    self.inner
                        .stream
                        .send_packet(StmtClose { statement: id })
                        .await?;
                }

                MySqlValueFormat::Binary
            } else {
                self.inner.stream.send_packet(Query(sql)).await?;
                self.inner.stream.in_command = false;

                MySqlValueFormat::Text
            }
        };

        loop {
            let mut packet = self.inner.stream.recv_packet().await?;

            if packet[0] == 0x00 || packet[0] == 0xff {
                let ok = packet.ok()?;

                self.inner.status_flags = ok.status;
                logger.increase_rows_affected(ok.affected_rows);

                if ok.status.contains(Status::SERVER_MORE_RESULTS_EXISTS) {
                    continue;
                }

                self.inner.stream.waiting.pop_front();
                return Ok(rows);
            }

            *self.inner.stream.waiting.front_mut().unwrap() = Waiting::Row;

            let num_columns = packet.get_uint_lenenc()?;
            let num_columns = usize::try_from(num_columns)
                .map_err(|_| err_protocol!("column count overflows usize: {num_columns}"))?;

            let column_names = match cached_statement {
                Some(id) => {
                    let inner = &mut *self.inner;
                    let (known_columns, column_names) = recv_result_metadata_reusing(
                        &mut inner.stream,
                        &mut inner.result_metadata,
                        id,
                        num_columns,
                    )
                    .await?;
                    columns = known_columns;
                    column_names
                }
                None => Arc::new(
                    recv_result_metadata(
                        &mut self.inner.stream,
                        num_columns,
                        Arc::make_mut(&mut columns),
                    )
                    .await?,
                ),
            };

            loop {
                let packet = self.inner.stream.recv_packet().await?;

                if packet[0] == 0xfe {
                    let status = if packet.len() < 9 {
                        packet.eof(self.inner.stream.capabilities)?.status
                    } else {
                        packet.ok()?.status
                    };

                    self.inner.status_flags = status;

                    if status.contains(Status::SERVER_MORE_RESULTS_EXISTS) {
                        *self.inner.stream.waiting.front_mut().unwrap() = Waiting::Result;
                        break;
                    }

                    self.inner.stream.waiting.pop_front();
                    return Ok(rows);
                }

                let row = match format {
                    MySqlValueFormat::Binary => packet.decode_with::<BinaryRow, _>(&columns)?.0,
                    MySqlValueFormat::Text => packet.decode_with::<TextRow, _>(&columns)?.0,
                };

                rows.push(MySqlRow {
                    row,
                    format,
                    columns: Arc::clone(&columns),
                    column_names: Arc::clone(&column_names),
                });

                logger.increment_rows_returned();
            }
        }
    }
}

impl<'c> Executor<'c> for &'c mut MySqlConnection {
    type Database = MySql;

    // granite patch: every row at once, without the row streams (`run_all`).
    fn fetch_all<'e, 'q, E>(self, mut query: E) -> BoxFuture<'e, Result<Vec<MySqlRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let arguments = query.take_arguments().map_err(Error::Encode);
        let persistent = query.persistent();

        Box::pin(async move {
            let sql = query.sql();
            let arguments = arguments?;

            self.run_all(sql, arguments, persistent).await
        })
    }

    fn fetch_many<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<MySqlQueryResult, MySqlRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let arguments = query.take_arguments().map_err(Error::Encode);
        let persistent = query.persistent();

        Box::pin(try_stream! {
        let sql = query.sql();
            let arguments = arguments?;
            let mut s = pin!(self.run(sql, arguments, persistent).await?);

            while let Some(v) = s.try_next().await? {
                r#yield!(v);
            }

            Ok(())
        })
    }

    fn fetch_optional<'e, 'q, E>(self, query: E) -> BoxFuture<'e, Result<Option<MySqlRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let mut s = self.fetch_many(query);

        Box::pin(async move {
            while let Some(v) = s.try_next().await? {
                if let Either::Right(r) = v {
                    return Ok(Some(r));
                }
            }

            Ok(None)
        })
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        _parameters: &'e [MySqlTypeInfo],
    ) -> BoxFuture<'e, Result<MySqlStatement, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            self.inner.stream.wait_until_ready().await?;
            // granite patch: see `MySqlStream::in_command`
            self.inner.stream.in_command = true;

            let metadata = if self.inner.cache_statement.is_enabled() {
                self.get_or_prepare_statement(sql.as_str()).await?.1
            } else {
                let (id, metadata) = self.prepare_statement(sql.as_str()).await?;

                self.inner
                    .stream
                    .send_packet(StmtClose { statement: id })
                    .await?;

                metadata
            };
            self.inner.stream.in_command = false;

            Ok(MySqlStatement {
                sql,
                // metadata has internal Arcs for expensive data structures
                metadata: metadata.clone(),
            })
        })
    }

    #[doc(hidden)]
    #[cfg(feature = "offline")]
    fn describe<'e>(
        self,
        sql: SqlStr,
    ) -> BoxFuture<'e, Result<crate::describe::Describe<MySql>, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            self.inner.stream.wait_until_ready().await?;
            // granite patch: see `MySqlStream::in_command`
            self.inner.stream.in_command = true;

            let (id, metadata) = self.prepare_statement(sql.as_str()).await?;

            self.inner
                .stream
                .send_packet(StmtClose { statement: id })
                .await?;
            self.inner.stream.in_command = false;

            let columns = (*metadata.columns).clone();

            let nullable = columns
                .iter()
                .map(|col| {
                    col.flags
                        .map(|flags| !flags.contains(crate::protocol::text::ColumnFlags::NOT_NULL))
                })
                .collect();

            Ok(crate::describe::Describe {
                parameters: Some(Either::Right(metadata.parameters)),
                columns,
                nullable,
            })
        })
    }
}

fn recv_next_result_column(def: &ColumnDefinition, ordinal: usize) -> Result<MySqlColumn, Error> {
    // if the alias is empty, use the alias
    // only then use the name
    let column_name = def.name()?;

    let name = match (def.name()?, def.alias()?) {
        (_, alias) if !alias.is_empty() => UStr::new(alias),
        (name, _) => UStr::new(name),
    };

    let table = def.table()?;

    let origin = if table.is_empty() {
        ColumnOrigin::Expression
    } else {
        let schema = def.schema()?;

        ColumnOrigin::Table(TableColumn {
            table: if !schema.is_empty() {
                format!("{schema}.{table}").into()
            } else {
                table.into()
            },
            name: column_name.into(),
        })
    };

    let type_info = MySqlTypeInfo::from_column(def);

    Ok(MySqlColumn {
        name,
        type_info,
        ordinal,
        flags: Some(def.flags),
        origin,
    })
}

async fn recv_result_metadata(
    stream: &mut MySqlStream,
    num_columns: usize,
    columns: &mut Vec<MySqlColumn>,
) -> Result<HashMap<UStr, usize>, Error> {
    // the result-set metadata is primarily a listing of each output
    // column in the result-set

    // MySQL (and MariaDB) enforce a hard limit of 4096 columns per result set;
    // reject larger claimed counts before they are used to size allocations,
    // as a hostile server could otherwise force an arbitrarily large allocation
    if num_columns > 4096 {
        return Err(err_protocol!(
            "result-set metadata claims {num_columns} columns but the maximum supported is 4096"
        ));
    }

    let mut column_names = HashMap::with_capacity(num_columns);

    columns.clear();
    columns.reserve(num_columns);

    for ordinal in 0..num_columns {
        let def: ColumnDefinition = stream.recv().await?;

        let column = recv_next_result_column(&def, ordinal)?;

        column_names.insert(column.name.clone(), ordinal);
        columns.push(column);
    }

    stream.maybe_recv_eof().await?;

    Ok(column_names)
}
