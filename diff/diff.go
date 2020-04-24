// Package diff contains functions to generate SQL statements to
// migrate an old schema to the new schema
package diff

import (
	"bytes"
	"io"
	"reflect"
	"sort"

	mapset "github.com/deckarep/golang-set"
	"github.com/schemalex/schemalex"
	"github.com/schemalex/schemalex/format"
	"github.com/schemalex/schemalex/internal/errors"
	"github.com/schemalex/schemalex/model"
)

type diffCtx struct {
	fromSet mapset.Set
	toSet   mapset.Set
	from    model.Stmts
	to      model.Stmts
}

func newDiffCtx(from, to model.Stmts) *diffCtx {
	fromSet := mapset.NewSet()
	for _, stmt := range from {
		if cs, ok := stmt.(model.Table); ok {
			fromSet.Add(cs.ID())
		}
	}
	toSet := mapset.NewSet()
	for _, stmt := range to {
		if cs, ok := stmt.(model.Table); ok {
			toSet.Add(cs.ID())
		}
	}

	return &diffCtx{
		fromSet: fromSet,
		toSet:   toSet,
		from:    from,
		to:      to,
	}
}

// Statements compares two model.Stmts and generates a series
// of statements to migrate from the old one to the new one,
// writing the result to `dst`
func Statements(dst io.Writer, from, to model.Stmts, options ...Option) error {
	var txn bool
	for _, o := range options {
		switch o.Name() {
		case optkeyTransaction:
			txn = o.Value().(bool)
		}
	}

	ctx := newDiffCtx(from, to)

	var procs = []func(*diffCtx, io.Writer) (int64, error){
		dropTables,
		createTables,
		alterTables,
	}

	var buf bytes.Buffer
	if txn {
		buf.WriteString("BEGIN;\n\nSET FOREIGN_KEY_CHECKS = 0;")
	}

	blank := true
	for _, p := range procs {
		var pbuf bytes.Buffer
		n, err := p(ctx, &pbuf)
		if err != nil {
			return errors.Wrap(err, `failed to produce diff`)
		}

		if pbuf.Len() == 0 {
			continue
		}

		blank = false

		if txn && n > 0 || !txn && buf.Len() > 0 && n > 0 {
			buf.WriteString("\n\n")
		}

		pbuf.WriteTo(&buf)
	}

	if txn {
		buf.WriteString("\n\nSET FOREIGN_KEY_CHECKS = 1;\n\nCOMMIT;")
	}

	if blank {
		buf.Reset()
	}

	if _, err := buf.WriteTo(dst); err != nil {
		return errors.Wrap(err, `failed to write diff`)
	}

	return nil
}

// Strings compares two strings and generates a series
// of statements to migrate from the old one to the new one,
// writing the result to `dst`
func Strings(dst io.Writer, from, to string, options ...Option) error {
	var p *schemalex.Parser
	for _, o := range options {
		switch o.Name() {
		case optkeyParser:
			p = o.Value().(*schemalex.Parser)
		}
	}
	if p == nil {
		p = schemalex.New()
	}

	stmts1, err := p.ParseString(from)
	if err != nil {
		return errors.Wrapf(err, `failed to parse "from" %s`, from)
	}

	stmts2, err := p.ParseString(to)
	if err != nil {
		return errors.Wrapf(err, `failed to parse "to" %s`, to)
	}

	return Statements(dst, stmts1, stmts2, options...)
}

// Files compares contents of two files and generates a series
// of statements to migrate from the old one to the new one,
// writing the result to `dst`
func Files(dst io.Writer, from, to string, options ...Option) error {
	return Sources(dst, schemalex.NewLocalFileSource(from), schemalex.NewLocalFileSource(to), options...)
}

// Sources compares contents from two sources and generates a series
// of statements to migrate from the old one to the new one,
// writing the result to `dst`
func Sources(dst io.Writer, from, to schemalex.SchemaSource, options ...Option) error {
	var buf bytes.Buffer
	if err := from.WriteSchema(&buf); err != nil {
		return errors.Wrapf(err, `failed to retrieve schema from "from" source %s`, from)
	}
	fromStr := buf.String()
	buf.Reset()

	if err := to.WriteSchema(&buf); err != nil {
		return errors.Wrapf(err, `failed to retrieve schema from "to" source %s`, to)
	}
	return Strings(dst, fromStr, buf.String(), options...)
}

func dropTables(ctx *diffCtx, dst io.Writer) (int64, error) {
	var buf bytes.Buffer
	ids := ctx.fromSet.Difference(ctx.toSet)
	for i, id := range ids.ToSlice() {
		if i > 0 {
			buf.WriteByte('\n')
		}

		stmt, ok := ctx.from.Lookup(id.(string))
		if !ok {
			return 0, errors.Errorf(`failed to lookup table %s`, id)
		}

		table, ok := stmt.(model.Table)
		if !ok {
			return 0, errors.Errorf(`lookup failed: %s is not a model.Table`, id)
		}
		buf.WriteString("DROP TABLE `")
		buf.WriteString(table.Name())
		buf.WriteString("`;")
	}

	return buf.WriteTo(dst)
}

func createTables(ctx *diffCtx, dst io.Writer) (int64, error) {
	var buf bytes.Buffer

	ids := ctx.toSet.Difference(ctx.fromSet)
	for _, id := range ids.ToSlice() {
		// Lookup the corresponding statement, and add its SQL
		stmt, ok := ctx.to.Lookup(id.(string))
		if !ok {
			return 0, errors.Errorf(`failed to lookup table %s`, id)
		}

		if buf.Len() > 0 {
			buf.WriteByte('\n')
		}

		if err := format.SQL(&buf, stmt); err != nil {
			return 0, err
		}
		buf.WriteByte(';')
	}
	return buf.WriteTo(dst)
}

type alterCtx struct {
	fromColumns mapset.Set
	toColumns   mapset.Set
	fromIndexes mapset.Set
	toIndexes   mapset.Set
	from        model.Table
	to          model.Table
}

func newAlterCtx(from, to model.Table) *alterCtx {
	fromColumns := mapset.NewSet()
	for col := range from.Columns() {
		fromColumns.Add(col.ID())
	}

	toColumns := mapset.NewSet()
	for col := range to.Columns() {
		toColumns.Add(col.ID())
	}

	fromIndexes := mapset.NewSet()
	for idx := range from.Indexes() {
		fromIndexes.Add(idx.ID())
	}

	toIndexes := mapset.NewSet()
	for idx := range to.Indexes() {
		toIndexes.Add(idx.ID())
	}

	return &alterCtx{
		fromColumns: fromColumns,
		toColumns:   toColumns,
		fromIndexes: fromIndexes,
		toIndexes:   toIndexes,
		from:        from,
		to:          to,
	}
}

func alterTables(ctx *diffCtx, dst io.Writer) (int64, error) {
	procs := []func(*alterCtx, io.Writer) (int64, error){
		alterTableOptions,
		dropTableIndexes,
		dropTableColumns,
		addTableColumns,
		alterTableColumns,
		addTableIndexes,
		alterTablePrimary,
	}

	ids := ctx.toSet.Intersect(ctx.fromSet)
	var buf bytes.Buffer
	for _, id := range ids.ToSlice() {
		var stmt model.Stmt
		var ok bool

		stmt, ok = ctx.from.Lookup(id.(string))
		if !ok {
			return 0, errors.Errorf(`table '%s' not found in old schema (alter table)`, id)
		}
		beforeStmt := stmt.(model.Table)

		stmt, ok = ctx.to.Lookup(id.(string))
		if !ok {
			return 0, errors.Errorf(`table '%s' not found in new schema (alter table)`, id)
		}
		afterStmt := stmt.(model.Table)

		var found bool
		var pbuf bytes.Buffer
		alterCtx := newAlterCtx(beforeStmt, afterStmt)
		for _, p := range procs {
			pbuf.Reset()

			n, err := p(alterCtx, &pbuf)
			if err != nil {
				return 0, errors.Wrap(err, `failed to generate alter table`)
			}

			if buf.Len() > 0 && n > 0 {
				buf.WriteByte('\n')
			}

			if pbuf.Len() > 0 {
				if !found {
					found = true

					buf.WriteString("ALTER TABLE `")
					buf.WriteString(alterCtx.from.Name())
					buf.WriteByte('`')
					buf.WriteByte('\n')
				}

				pbuf.WriteTo(&buf)
			}
		}

		if found {
			buf.WriteByte(';')
		}
	}

	return buf.WriteTo(dst)
}

func dropTableColumns(ctx *alterCtx, dst io.Writer) (int64, error) {
	columnNames := ctx.fromColumns.Difference(ctx.toColumns)

	var buf bytes.Buffer
	for _, columnName := range columnNames.ToSlice() {
		if buf.Len() > 0 {
			buf.WriteString(",\n")
		}
		// buf.WriteString("ALTER TABLE `")
		// buf.WriteString(ctx.from.Name())
		buf.WriteString("    DROP COLUMN `")
		col, ok := ctx.from.LookupColumn(columnName.(string))
		if !ok {
			return 0, errors.Errorf(`failed to lookup column %s`, columnName)
		}

		buf.WriteString(col.Name())

		buf.WriteByte('`')
	}

	return buf.WriteTo(dst)
}

func addTableColumns(ctx *alterCtx, dst io.Writer) (int64, error) {
	var buf bytes.Buffer

	beforeToNext := make(map[string]string) // lookup next column
	nextToBefore := make(map[string]string) // lookup before column

	// In order to do this correctly, we need to create a graph so that
	// we always start adding with a column that has a either no before
	// columns, or one that already exists in the database
	var firstColumn model.TableColumn
	for _, v := range ctx.toColumns.Difference(ctx.fromColumns).ToSlice() {
		columnName := v.(string)
		// find the before-column for each.
		col, ok := ctx.to.LookupColumn(columnName)
		if !ok {
			return 0, errors.Errorf(`failed to lookup column %s`, columnName)
		}

		beforeCol, hasBeforeCol := ctx.to.LookupColumnBefore(col.ID())
		if !hasBeforeCol {
			// if there is no before-column, then this is a special "FIRST"
			// column
			firstColumn = col
			continue
		}

		// otherwise, keep a reverse-lookup map of before -> next columns
		beforeToNext[beforeCol.ID()] = columnName
		nextToBefore[columnName] = beforeCol.ID()
	}

	// First column is always safe to add
	if firstColumn != nil {
		writeAddColumn(ctx, &buf, firstColumn.ID())
	}

	var columnNames []string
	// Find columns that have before columns which existed in both
	// from and to tables
	for _, v := range ctx.toColumns.Intersect(ctx.fromColumns).ToSlice() {
		columnName := v.(string)
		if nextColumnName, ok := beforeToNext[columnName]; ok {
			delete(beforeToNext, columnName)
			delete(nextToBefore, nextColumnName)
			columnNames = append(columnNames, nextColumnName)
		}
	}

	if len(columnNames) > 0 {
		sort.Strings(columnNames)
		writeAddColumn(ctx, &buf, columnNames...)
	}

	// Finally, we process the remaining columns.
	// All remaining columns are new, and they will depend on a
	// newly created column. This means we have to make sure to
	// create them in the order that they are dependent on.
	columnNames = columnNames[:0]
	for _, nextCol := range beforeToNext {
		columnNames = append(columnNames, nextCol)
	}
	// if there's one left, that can be appended
	if len(columnNames) > 0 {
		sort.Slice(columnNames, func(i, j int) bool {
			icol, _ := ctx.to.LookupColumnOrder(columnNames[i])
			jcol, _ := ctx.to.LookupColumnOrder(columnNames[j])
			return icol < jcol
		})
		writeAddColumn(ctx, &buf, columnNames...)
	}
	return buf.WriteTo(dst)
}

func writeAddColumn(ctx *alterCtx, buf *bytes.Buffer, columnNames ...string) error {
	for _, columnName := range columnNames {
		stmt, ok := ctx.to.LookupColumn(columnName)
		if !ok {
			return errors.Errorf(`failed to lookup column %s`, columnName)
		}

		beforeCol, hasBeforeCol := ctx.to.LookupColumnBefore(stmt.ID())
		if buf.Len() > 0 {
			buf.WriteString(",\n")
		}
		// buf.WriteString("ALTER TABLE `")
		// buf.WriteString(ctx.from.Name())
		buf.WriteString("    ADD COLUMN ")
		if err := format.SQL(buf, stmt); err != nil {
			return err
		}
		if hasBeforeCol {
			buf.WriteString(" AFTER `")
			buf.WriteString(beforeCol.Name())
			buf.WriteString("`")
		} else {
			buf.WriteString(" FIRST")
		}
	}
	return nil
}

func alterTableColumns(ctx *alterCtx, dst io.Writer) (int64, error) {
	var buf bytes.Buffer
	columnNames := ctx.toColumns.Intersect(ctx.fromColumns)
	for _, columnName := range columnNames.ToSlice() {
		beforeColumnStmt, ok := ctx.from.LookupColumn(columnName.(string))
		if !ok {
			return 0, errors.Errorf(`column %s not found in old schema`, columnName)
		}

		afterColumnStmt, ok := ctx.to.LookupColumn(columnName.(string))
		if !ok {
			return 0, errors.Errorf(`column %s not found in new schema`, columnName)
		}

		// if (beforeColumnStmt.HasCharacterSet() || ctx.from.HasCharacterSet()) && (ctx.to.HasCharacterSet() || afterColumnStmt.HasCharacterSet()) {
		// 	beforeColumnCharset := ctx.from.CharacterSet()
		// 	if beforeColumnStmt.HasCharacterSet() {
		// 		beforeColumnCharset = beforeColumnStmt.CharacterSet()
		// 	}

		// 	afterColumnCharset := ctx.to.CharacterSet()
		// 	if afterColumnStmt.HasCharacterSet() {
		// 		afterColumnCharset = afterColumnStmt.CharacterSet()
		// 	}

		// 	if beforeColumnCharset != afterColumnCharset {
		// 		afterColumnStmt.SetCharacterSet(afterColumnCharset)
		// 	}
		// }

		if (beforeColumnStmt.HasCollation() || ctx.to.HasCollation()) && (ctx.to.HasCollation() || afterColumnStmt.HasCollation()) {
			beforeColumnCollation := ctx.from.Collation()
			if beforeColumnStmt.HasCollation() {
				beforeColumnCollation = beforeColumnStmt.Collation()
			}

			afterColumnCollation := ctx.to.Collation()
			if afterColumnStmt.HasCollation() {
				afterColumnCollation = afterColumnStmt.Collation()
			}

			if beforeColumnCollation != afterColumnCollation || beforeColumnStmt.HasCollation() {
				afterColumnStmt.SetCollation(afterColumnCollation)
			}
		}

		if reflect.DeepEqual(beforeColumnStmt, afterColumnStmt) {
			continue
		}

		if buf.Len() > 0 {
			buf.WriteString(",\n")
		}
		// buf.WriteString("ALTER TABLE `")
		// buf.WriteString(ctx.from.Name())
		buf.WriteString("    CHANGE COLUMN `")
		buf.WriteString(afterColumnStmt.Name())
		buf.WriteString("` ")
		if err := format.SQL(&buf, afterColumnStmt); err != nil {
			return 0, err
		}
	}

	return buf.WriteTo(dst)
}

func dropTableIndexes(ctx *alterCtx, dst io.Writer) (int64, error) {
	var buf bytes.Buffer
	indexes := ctx.fromIndexes.Difference(ctx.toIndexes)
	// drop index after drop constraint.
	// because cannot drop index if needed in a foreign key constraint
	lazy := make([]model.Index, 0, indexes.Cardinality())
	for _, index := range indexes.ToSlice() {
		indexStmt, ok := ctx.from.LookupIndex(index.(string))
		if !ok {
			return 0, errors.Errorf(`index '%s' not found in old schema (drop index)`, index)
		}

		if indexStmt.IsPrimaryKey() {
			continue
		}

		if !indexStmt.HasName() && !indexStmt.HasSymbol() {
			return 0, errors.Errorf("can not drop index without name: %s", indexStmt.ID())
		}
		if !indexStmt.IsForeignKey() {
			lazy = append(lazy, indexStmt)
			continue
		}

		if buf.Len() > 0 {
			buf.WriteString(",\n")
		}
		// buf.WriteString("ALTER TABLE `")
		// buf.WriteString(ctx.from.Name())
		buf.WriteString("    DROP FOREIGN KEY `")
		if indexStmt.HasSymbol() {
			buf.WriteString(indexStmt.Symbol())
		} else {
			buf.WriteString(indexStmt.Name())
		}
		buf.WriteByte('`')
	}
	// drop index after drop CONSTRAINT
	for _, indexStmt := range lazy {
		if buf.Len() > 0 {
			buf.WriteString(",\n")
		}
		// buf.WriteString("ALTER TABLE `")
		// buf.WriteString(ctx.from.Name())
		buf.WriteString("    DROP INDEX `")
		if !indexStmt.HasName() {
			buf.WriteString(indexStmt.Symbol())
		} else {
			buf.WriteString(indexStmt.Name())
		}
		buf.WriteByte('`')
	}

	return buf.WriteTo(dst)
}

func addTableIndexes(ctx *alterCtx, dst io.Writer) (int64, error) {
	var buf bytes.Buffer
	indexes := ctx.toIndexes.Difference(ctx.fromIndexes)
	// add index before add foreign key.
	// because cannot add index if create implicitly index by foreign key.
	lazy := make([]model.Index, 0, indexes.Cardinality())
	for _, index := range indexes.ToSlice() {
		indexStmt, ok := ctx.to.LookupIndex(index.(string))
		if !ok {
			return 0, errors.Errorf(`index '%s' not found in old schema (add index)`, index)
		}

		if indexStmt.IsPrimaryKey() {
			continue
		}

		if indexStmt.IsForeignKey() {
			lazy = append(lazy, indexStmt)
			continue
		}

		if buf.Len() > 0 {
			buf.WriteString(",\n")
		}
		// buf.WriteString("ALTER TABLE `")
		// buf.WriteString(ctx.from.Name())
		buf.WriteString("    ADD ")
		if err := format.SQL(&buf, indexStmt); err != nil {
			return 0, err
		}
	}

	for _, indexStmt := range lazy {
		if buf.Len() > 0 {
			buf.WriteString(",\n")
		}
		// buf.WriteString("ALTER TABLE `")
		// buf.WriteString(ctx.from.Name())
		buf.WriteString("    ADD ")
		if err := format.SQL(&buf, indexStmt); err != nil {
			return 0, err
		}
	}

	return buf.WriteTo(dst)
}

func alterTablePrimary(ctx *alterCtx, dst io.Writer) (int64, error) {
	var buf bytes.Buffer

	found := false
	indexes := ctx.fromIndexes.Difference(ctx.toIndexes)
	for _, index := range indexes.ToSlice() {
		indexStmt, ok := ctx.from.LookupIndex(index.(string))
		if !ok {
			return 0, errors.Errorf(`index '%s' not found in old schema (drop index)`, index)
		}

		if indexStmt.IsPrimaryKey() {
			found = true
			break
		}
	}

	indexes = ctx.toIndexes.Difference(ctx.fromIndexes)
	for _, index := range indexes.ToSlice() {
		indexStmt, ok := ctx.to.LookupIndex(index.(string))
		if !ok {
			return 0, errors.Errorf(`index '%s' not found in old schema (add index)`, index)
		}

		if indexStmt.IsPrimaryKey() {
			if buf.Len() > 0 {
				buf.WriteString(",\n")
			}
			// buf.WriteString("ALTER TABLE `")
			// buf.WriteString(ctx.from.Name())
			if found {
				buf.WriteString("   DROP PRIMARY KEY, ADD ")
				found = false
			} else {
				buf.WriteString("   ADD ")
			}

			if err := format.SQL(&buf, indexStmt); err != nil {
				return 0, err
			}

			break
		}
	}

	if found {
		if buf.Len() > 0 {
			buf.WriteString(",\n")
		}
		// buf.WriteString("ALTER TABLE `")
		// buf.WriteString(ctx.from.Name())
		buf.WriteString("   DROP PRIMARY KEY")
	}

	return buf.WriteTo(dst)
}

func alterTableOptions(ctx *alterCtx, dst io.Writer) (int64, error) {
	var buf bytes.Buffer
	for afterOptStmt := range ctx.to.Options() {
		beforeOptStmt, ok := ctx.from.LookupOption(afterOptStmt.ID())
		if ok {
			if reflect.DeepEqual(beforeOptStmt, afterOptStmt) {
				continue
			}
		}

		if buf.Len() > 0 {
			buf.WriteString(",\n")
		}
		// buf.WriteString("ALTER TABLE `")
		// buf.WriteString(ctx.from.Name())
		buf.WriteString("    ")
		if err := format.SQL(&buf, afterOptStmt); err != nil {
			return 0, err
		}
	}

	return buf.WriteTo(dst)
}
