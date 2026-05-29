using SeniorTicker.Domain;

namespace SeniorTicker.Application;

/// <summary>
/// Дедупликатор одного шарда. КОНТРАКТ: вызывается ровно из одного потока-потребителя
/// (single-writer), поэтому реализация НЕ обязана быть потокобезопасной — это сознательный
/// выбор, убирающий гонку №1 из код-ревью по конструкции (нет общего состояния между потоками).
/// </summary>
public interface IDeduplicator
{
    /// <returns>true — тик уже виден в окне (дубликат, отбросить); false — новый.</returns>
    bool IsDuplicate(in Tick tick);
}
