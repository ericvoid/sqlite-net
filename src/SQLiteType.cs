#if WINDOWS_PHONE && !USE_WP8_NATIVE_SQLITE
#define USE_CSHARP_SQLITE
#endif

using System;
using System.Collections.Generic;

#if USE_CSHARP_SQLITE
using Sqlite3Statement = Community.CsharpSqlite.Sqlite3.Vdbe;
#elif USE_WP8_NATIVE_SQLITE
using Sqlite3Statement = Sqlite.Statement;
#else
using Sqlite3Statement = System.IntPtr;
using System.Linq;
#endif

namespace SQLite 
{
	
	internal class TypeAdapterFactory
	{
		// SINGLETON DEFINITION
		private static volatile TypeAdapterFactory instance;
		private static object syncRoot = new Object();
		
		public static TypeAdapterFactory Instance
		{
			get 
			{
				if (instance == null) 
				{
					lock (syncRoot) 
					{
						if (instance == null) 
							instance = new TypeAdapterFactory();
					}
				}
				
				return instance;
			}
		}
		
		
		private Dictionary<Type, TypeAdapter> Adapters;
		
		private TypeAdapter EnumAdapter;
		
		private TypeAdapterFactory() {
			this.Adapters = new Dictionary<Type, TypeAdapter>();
			SetupAdapters();
		}
		
		private void SetupAdapters() 
		{
			EnumAdapter = new EnumTypeAdapter();
			
			var adapters = TypeLoader.TypesImplementingInterface<TypeAdapter>();
			
			foreach (var adapter in adapters) {
				Adapters[adapter.GetCsharpType()] = adapter;
			}
		}
		
		internal TypeAdapter GetAdapter<T>(TypeAdapterOptions options=null) {
			return GetAdapter(typeof(T), options);
		}
		
		internal TypeAdapter GetAdapter(Type type, TypeAdapterOptions options=null)
		{
			TypeAdapter result = null;
			
			if (Adapters.TryGetValue(type, out result)) {
				result.options = options;
				return result;
			}
			
#if !NETFX_CORE
			if (type.IsEnum) {
#else
			if (type.GetTypeInfo().IsEnum) {
#endif
				EnumAdapter.options = options;
				return EnumAdapter;
			}
			
			throw new NotSupportedException ("No adapter for type " + type);
		}
				
	}
	
	
	
	internal abstract class TypeAdapter
	{
		internal TypeAdapterOptions options { get; set; }
			
		public abstract Type GetCsharpType();
			
		public abstract String GetSqlType();
			
		public abstract void BindParameter(Sqlite3Statement stmt, int index, object value);
			
		public abstract object ReadColumn(Sqlite3Statement stmt, int index);
			
	}
	
		
	internal class BooleanTypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(Boolean);
		}
		
		public override string GetSqlType()
		{
			return "INTEGER";
		}
		
		public override void BindParameter(Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindInt(stmt, index, (bool)value ? 1 : 0);
		}
		
		public override object ReadColumn(Sqlite3Statement stmt, int index) 
		{
			return SQLite3.ColumnInt(stmt, index) == 1;
		}
	}
	
	
	internal class ByteTypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(Byte);
		}
		
		public override string GetSqlType()
		{
			return "INTEGER";
		}
		
		public override void BindParameter (Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindInt(stmt, index, Convert.ToInt32(value));
		}
		
		public override object ReadColumn (Sqlite3Statement stmt, int index) 
		{
			return (byte)SQLite3.ColumnInt (stmt, index);
		}
	}
	
	
	internal class UInt16TypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(UInt16);
		}
		
		public override string GetSqlType()
		{
			return "INTEGER";
		}
		
		public override void BindParameter (Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindInt(stmt, index, Convert.ToInt32(value));
		}
		
		public override object ReadColumn (Sqlite3Statement stmt, int index) 
		{
			return (ushort)SQLite3.ColumnInt (stmt, index);
		}
	}
		
		
	internal class SByteTypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(SByte);
		}
		
		public override string GetSqlType()
		{
			return "INTEGER";
		}
		
		public override void BindParameter(Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindInt(stmt, index, Convert.ToInt32(value));
		}
		
		public override object ReadColumn(Sqlite3Statement stmt, int index) 
		{
			return (sbyte)SQLite3.ColumnInt(stmt, index);
		}
	}
	
		
	internal class Int16TypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(Int16);
		}
		
		public override string GetSqlType()
		{
			return "INTEGER";
		}
		
		public override void BindParameter(Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindInt(stmt, index, Convert.ToInt32(value));
		}
		
		public override object ReadColumn(Sqlite3Statement stmt, int index) 
		{
			return (short)SQLite3.ColumnInt(stmt, index);
		}
	}
		
		
	internal class Int32TypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(Int32);
		}
		
		public override string GetSqlType()
		{
			return "INTEGER";
		}
		
		public override void BindParameter(Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindInt(stmt, index, (int)value);
		}
		
		public override object ReadColumn (Sqlite3Statement stmt, int index) 
		{
			return (int)SQLite3.ColumnInt (stmt, index);
		}
	}
		
	
	internal class UInt32TypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(UInt32);
		}
		
		public override string GetSqlType()
		{
			return "BIGINT";
		}
		
		public override void BindParameter(Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindInt64(stmt, index, Convert.ToInt64(value));
		}
		
		public override object ReadColumn(Sqlite3Statement stmt, int index) 
		{
			return (uint)SQLite3.ColumnInt64(stmt, index);
		}
	}
		
		
	internal class Int64TypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(Int64);
		}
		
		public override string GetSqlType()
		{
			return "BIGINT";
		}
		
		public override void BindParameter (Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindInt64(stmt, index, Convert.ToInt64(value));
		}
		
		public override object ReadColumn (Sqlite3Statement stmt, int index) 
		{
			return SQLite3.ColumnInt64(stmt, index);
		}
	}
		

	internal class SingleTypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(Single);
		}
		
		public override string GetSqlType()
		{
			return "FLOAT";
		}
		
		public override void BindParameter(Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindDouble(stmt, index, Convert.ToDouble(value));
		}
		
		public override object ReadColumn(Sqlite3Statement stmt, int index) 
		{
			return (float)SQLite3.ColumnDouble(stmt, index);
		}
	}
		
	
	internal class DoubleTypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(Double);
		}
		
		public override string GetSqlType()
		{
			return "FLOAT";
		}
		
		public override void BindParameter(Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindDouble(stmt, index, Convert.ToDouble(value));
		}
		
		public override object ReadColumn(Sqlite3Statement stmt, int index) 
		{
			return SQLite3.ColumnDouble(stmt, index);
		}
	}
		
	
	internal class DecimalTypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(Decimal);
		}
		
		public override string GetSqlType()
		{
			return "FLOAT";
		}
		
		public override void BindParameter(Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindDouble(stmt, index, Convert.ToDouble(value));
		}
		
		public override object ReadColumn(Sqlite3Statement stmt, int index) 
		{
			return (decimal)SQLite3.ColumnDouble(stmt, index);
		}
	}
		
		
	internal class StringTypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(String);
		}
		
		public override string GetSqlType()
		{
			return "VARCHAR(" + options.MaxLength + ")";
		}
		
		public override void BindParameter(Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindText(stmt, index, (string)value, -1, options.NegativePointer);
		}
		
		public override object ReadColumn(Sqlite3Statement stmt, int index) 
		{
			return SQLite3.ColumnString(stmt, index);
		}
	}
		

	internal class GuidTypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(Guid);
		}
		
		public override string GetSqlType()
		{
			return "VARCHAR(36)";
		}
		
		public override void BindParameter (Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindText(stmt, index, ((Guid)value).ToString(), 72, options.NegativePointer);
		}
		
		public override object ReadColumn (Sqlite3Statement stmt, int index) 
		{
			var text = SQLite3.ColumnString(stmt, index);
        	return new Guid(text);
		}
	}
		
		
	internal class DateTimeTypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(DateTime);
		}
		
		public override string GetSqlType()
		{
			return options.StoreDateTimeAsTicks ? "bigint" : "datetime";
		}
		
		public override void BindParameter (Sqlite3Statement stmt, int index, object value) 
		{
			if (options.StoreDateTimeAsTicks) {
				SQLite3.BindInt64(stmt, index, ((DateTime)value).Ticks);
			}
			else {
				SQLite3.BindText(stmt, index, ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss"), -1, options.NegativePointer);
			}
		}
		
		public override object ReadColumn (Sqlite3Statement stmt, int index) 
		{
			if (options.StoreDateTimeAsTicks)
			{
				return new DateTime(SQLite3.ColumnInt64(stmt, index));
			}
			else 
			{
				return DateTime.Parse(SQLite3.ColumnString(stmt, index));
			}
		}
	}
	
		
	internal class ByteArrayTypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(byte[]);
		}
		
		public override string GetSqlType()
		{
			return "BLOB";
		}
		
		public override void BindParameter(Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindBlob(stmt, index, (byte[]) value, ((byte[])value).Length, options.NegativePointer);
		}
		
		public override object ReadColumn(Sqlite3Statement stmt, int index) 
		{
			return SQLite3.ColumnByteArray(stmt, index);
		}
	}
		
		
	internal class EnumTypeAdapter : TypeAdapter
	{
		public override Type GetCsharpType() {
			return typeof(Enum);
		}
		
		public override string GetSqlType()
		{
			return "INTEGER";
		}
		
		public override void BindParameter (Sqlite3Statement stmt, int index, object value) 
		{
			SQLite3.BindInt(stmt, index, Convert.ToInt32(value));
		}
		
		public override object ReadColumn (Sqlite3Statement stmt, int index) 
		{
			return SQLite3.ColumnInt(stmt, index);
		}
	}
	
		
	internal class TypeAdapterOptions
	{
		public bool StoreDateTimeAsTicks { get; set; }
		public int MaxLength { get; set; }
		
		public IntPtr NegativePointer { get; set; }
	}
		
		
	internal class TypeLoader
	{
		private static bool DoesTypeSupportInterface(Type type,Type inter)
		{
		    if(inter.IsAssignableFrom(type))
		        return true;
		    if(type.GetInterfaces().Any(i=>i. IsGenericType && i.GetGenericTypeDefinition()==inter))
		        return true;
		    return false;
		}
		
		public static IEnumerable<T> TypesImplementingInterface<T>()
		{
			Type desiredType = typeof(T);
			
		    return AppDomain
		        .CurrentDomain
		        .GetAssemblies()
		        .SelectMany(assembly => assembly.GetTypes())
		        .Where(type => DoesTypeSupportInterface(type, desiredType) 
						    && type.GetConstructor(Type.EmptyTypes) != null)
				.Select(type => (T)Activator.CreateInstance(type));
		
		}	
	}
}