using System;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;

namespace SQLite
{
	
	public class TableMapping
	{
		public Type MappedType { get; private set; }

		public string TableName { get; private set; }

		public Column[] Columns { get; private set; }

		public Column PK { get; private set; }

		public string GetByPrimaryKeySql { get; private set; }

		Column _autoPk;
		Column[] _insertColumns;
		Column[] _insertOrReplaceColumns;

        public TableMapping(Type type, CreateFlags createFlags = CreateFlags.None)
		{
			MappedType = type;

#if NETFX_CORE
			var tableAttr = (TableAttribute)System.Reflection.CustomAttributeExtensions
                .GetCustomAttribute(type.GetTypeInfo(), typeof(TableAttribute), true);
#else
			var tableAttr = (TableAttribute)type.GetCustomAttributes (typeof (TableAttribute), true).FirstOrDefault ();
#endif

			TableName = tableAttr != null ? tableAttr.Name : MappedType.Name;

#if !NETFX_CORE
			var props = MappedType.GetProperties (BindingFlags.Public | BindingFlags.Instance | BindingFlags.SetProperty);
#else
			var props = from p in MappedType.GetRuntimeProperties()
						where ((p.GetMethod != null && p.GetMethod.IsPublic) || (p.SetMethod != null && p.SetMethod.IsPublic) || (p.GetMethod != null && p.GetMethod.IsStatic) || (p.SetMethod != null && p.SetMethod.IsStatic))
						select p;
#endif
			var cols = new List<Column> ();
			foreach (var p in props) {
#if !NETFX_CORE
				var ignore = p.GetCustomAttributes (typeof(IgnoreAttribute), true).Length > 0;
#else
				var ignore = p.GetCustomAttributes (typeof(IgnoreAttribute), true).Count() > 0;
#endif
				if (p.CanWrite && !ignore) {
					cols.Add (new Column (p, createFlags));
				}
			}
			Columns = cols.ToArray ();
			foreach (var c in Columns) {
				if (c.IsAutoInc && c.IsPK) {
					_autoPk = c;
				}
				if (c.IsPK) {
					PK = c;
				}
			}
			
			HasAutoIncPK = _autoPk != null;

			if (PK != null) {
				GetByPrimaryKeySql = string.Format ("select * from \"{0}\" where \"{1}\" = ?", TableName, PK.Name);
			}
			else {
				// People should not be calling Get/Find without a PK
				GetByPrimaryKeySql = string.Format ("select * from \"{0}\" limit 1", TableName);
			}
		}

		public bool HasAutoIncPK { get; private set; }

		public void SetAutoIncPK (object obj, long id)
		{
			if (_autoPk != null) {
				_autoPk.SetValue (obj, Convert.ChangeType (id, _autoPk.ColumnType, null));
			}
		}

		public Column[] InsertColumns {
			get {
				if (_insertColumns == null) {
					_insertColumns = Columns.Where (c => !c.IsAutoInc).ToArray ();
				}
				return _insertColumns;
			}
		}

		public Column[] InsertOrReplaceColumns {
			get {
				if (_insertOrReplaceColumns == null) {
					_insertOrReplaceColumns = Columns.ToArray ();
				}
				return _insertOrReplaceColumns;
			}
		}

		public Column FindColumnWithPropertyName (string propertyName)
		{
			var exact = Columns.FirstOrDefault (c => c.PropertyName == propertyName);
			return exact;
		}

		public Column FindColumn (string columnName)
		{
			var exact = Columns.FirstOrDefault (c => c.Name == columnName);
			return exact;
		}
		
		PreparedSqlLiteInsertCommand _insertCommand;
		string _insertCommandExtra;

		public PreparedSqlLiteInsertCommand GetInsertCommand(SQLiteConnection conn, string extra)
		{
			if (_insertCommand == null) {
				_insertCommand = CreateInsertCommand(conn, extra);
				_insertCommandExtra = extra;
			}
			else if (_insertCommandExtra != extra) {
				_insertCommand.Dispose();
				_insertCommand = CreateInsertCommand(conn, extra);
				_insertCommandExtra = extra;
			}
			return _insertCommand;
		}
		
		PreparedSqlLiteInsertCommand CreateInsertCommand(SQLiteConnection conn, string extra)
		{
			var cols = InsertColumns;
		    string insertSql;
            if (!cols.Any() && Columns.Count() == 1 && Columns[0].IsAutoInc)
            {
                insertSql = string.Format("insert {1} into \"{0}\" default values", TableName, extra);
            }
            else
            {
				var replacing = string.Compare (extra, "OR REPLACE", StringComparison.OrdinalIgnoreCase) == 0;

				if (replacing) {
					cols = InsertOrReplaceColumns;
				}

                insertSql = string.Format("insert {3} into \"{0}\"({1}) values ({2})", TableName,
                                   string.Join(",", (from c in cols
                                                     select "\"" + c.Name + "\"").ToArray()),
                                   string.Join(",", (from c in cols
                                                     select "?").ToArray()), extra);
                
            }
			
			var insertCommand = new PreparedSqlLiteInsertCommand(conn);
			insertCommand.CommandText = insertSql;
			return insertCommand;
		}
		
		protected internal void Dispose()
		{
			if (_insertCommand != null) {
				_insertCommand.Dispose();
				_insertCommand = null;
			}
		}

	}


	public class Column
	{
		public const int DefaultMaxStringLength = 140;
        public const string ImplicitPkName = "Id";
    	public const string ImplicitIndexSuffix = "Id";
		
		PropertyInfo _prop;

		public string Name { get; private set; }

		public string PropertyName { get { return _prop.Name; } }

		public Type ColumnType { get; private set; }

		public string Collation { get; private set; }

        public bool IsAutoInc { get; private set; }
        public bool IsAutoGuid { get; private set; }

		public bool IsPK { get; private set; }

		public IEnumerable<IndexedAttribute> Indices { get; set; }

		public bool IsNullable { get; private set; }

		public int MaxStringLength { get; private set; }
		

        internal Column(PropertyInfo prop, CreateFlags createFlags = CreateFlags.None)
        {
            var colAttr = (ColumnAttribute)prop.GetCustomAttributes(typeof(ColumnAttribute), true).FirstOrDefault();

            _prop = prop;
            Name = colAttr == null ? prop.Name : colAttr.Name;
            //If this type is Nullable<T> then Nullable.GetUnderlyingType returns the T, otherwise it returns null, so get the actual type instead
            ColumnType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
            Collation = GetCollation(prop);

            IsPK = HasPkAttribute(prop) ||
				(((createFlags & CreateFlags.ImplicitPK) == CreateFlags.ImplicitPK) &&
				 	string.Compare (prop.Name, Column.ImplicitPkName, StringComparison.OrdinalIgnoreCase) == 0);

            var isAuto = HasAutoIncAttribute(prop) || (IsPK && ((createFlags & CreateFlags.AutoIncPK) == CreateFlags.AutoIncPK));
            IsAutoGuid = isAuto && ColumnType == typeof(Guid);
            IsAutoInc = isAuto && !IsAutoGuid;

            Indices = GetIndices(prop);
            if (!Indices.Any()
                && !IsPK
                && ((createFlags & CreateFlags.ImplicitIndex) == CreateFlags.ImplicitIndex)
                && Name.EndsWith (Column.ImplicitIndexSuffix, StringComparison.OrdinalIgnoreCase)
                )
            {
                Indices = new IndexedAttribute[] { new IndexedAttribute() };
            }
            IsNullable = !IsPK;
            MaxStringLength = GetMaxStringLength(prop);
        }

		public void SetValue (object obj, object val)
		{
			_prop.SetValue (obj, val, null);
		}

		public object GetValue (object obj)
		{
			return _prop.GetValue (obj, null);
		}
		

		private bool HasPkAttribute (MemberInfo p)
		{
			var attrs = p.GetCustomAttributes (typeof(PrimaryKeyAttribute), true);
#if !NETFX_CORE
			return attrs.Length > 0;
#else
			return attrs.Count() > 0;
#endif
		}

		private string GetCollation (MemberInfo p)
		{
			var attrs = p.GetCustomAttributes (typeof(CollationAttribute), true);
#if !NETFX_CORE
			if (attrs.Length > 0) {
				return ((CollationAttribute)attrs [0]).Value;
#else
			if (attrs.Count() > 0) {
                return ((CollationAttribute)attrs.First()).Value;
#endif
			} else {
				return string.Empty;
			}
		}

		private bool HasAutoIncAttribute (MemberInfo p)
		{
			var attrs = p.GetCustomAttributes (typeof(AutoIncrementAttribute), true);
#if !NETFX_CORE
			return attrs.Length > 0;
#else
			return attrs.Count() > 0;
#endif
		}

		private static IEnumerable<IndexedAttribute> GetIndices(MemberInfo p)
		{
			var attrs = p.GetCustomAttributes(typeof(IndexedAttribute), true);
			return attrs.Cast<IndexedAttribute>();
		}
		
		private int GetMaxStringLength(PropertyInfo p)
		{
			var attrs = p.GetCustomAttributes (typeof(MaxLengthAttribute), true);
#if !NETFX_CORE
			if (attrs.Length > 0) {
				return ((MaxLengthAttribute)attrs [0]).Value;
#else
			if (attrs.Count() > 0) {
				return ((MaxLengthAttribute)attrs.First()).Value;
#endif
			} else {
				return Column.DefaultMaxStringLength;
			}
		}
				
		internal string SqlDecl (bool storeDateTimeAsTicks)
		{
			string decl = "\"" + this.Name + "\" " + this.SqlType(storeDateTimeAsTicks) + " ";
			
			if (IsPK) {
				decl += "primary key ";
			}
			if (IsAutoInc) {
				decl += "autoincrement ";
			}
			if (!IsNullable) {
				decl += "not null ";
			}
			if (!string.IsNullOrEmpty (this.Collation)) {
				decl += "collate " + this.Collation + " ";
			}
			
			return decl;
		}
				
		private string SqlType (bool storeDateTimeAsTicks)
		{
			var clrType = this.ColumnType;
					
			if (clrType == typeof(Boolean) || clrType == typeof(Byte) || clrType == typeof(UInt16) || clrType == typeof(SByte) || clrType == typeof(Int16) || clrType == typeof(Int32)) {
				return "integer";
			} else if (clrType == typeof(UInt32) || clrType == typeof(Int64)) {
				return "bigint";
			} else if (clrType == typeof(Single) || clrType == typeof(Double) || clrType == typeof(Decimal)) {
				return "float";
			} else if (clrType == typeof(String)) {
				int len = this.MaxStringLength;
				return "varchar(" + len + ")";
						
			} else if (clrType == typeof(DateTime)) {
				return storeDateTimeAsTicks ? "bigint" : "datetime";
						
#if !NETFX_CORE
			} else if (clrType.IsEnum) {
#else
			} else if (clrType.GetTypeInfo().IsEnum) {
#endif
				return "integer";
			} else if (clrType == typeof(byte[])) {
				return "blob";
            } else if (clrType == typeof(Guid)) {
                return "varchar(36)";
            } else {
				throw new NotSupportedException ("Don't know about " + clrType);
			}
		}
	} 
			
}